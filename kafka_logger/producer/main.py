from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import socket

app = FastAPI()

# 1. Kafka config: Broker
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}
producer = Producer(conf)

class LogMessage(BaseModel):
    level: str
    message: str

@app.post("/log")
async def log_message(log: LogMessage):
    print("Prajwal got one requies ...", log)
    try:
        producer.produce("log_topic", key=log.level, value=log.message)
        producer.flush()
        return {"status": "Message sent to Kafka"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
  
  
  
# 2. Elastic Search block ... 
from elasticsearch import Elasticsearch, helpers
es = Elasticsearch("http://localhost:9200")

class Product(BaseModel):
    name: str
    category: str
    price: float

@app.post("/addProducts")
def add_product(product: Product):
    print("Prajwal, you are adding this product", product)
    if not es.ping():
        raise HTTPException(status_code=500, detail="Elasticsearch not available")
    res = es.index(index="products", document=product.dict())
    return {"result": res["result"], "id": res["_id"]}

@app.get("/search/")
def search_product( name: str):
    if not es.ping():
        raise HTTPException(status_code=500, detail="Elasticsearch not available")
    res = es.search(index="products", query={"match": {"name": name}})
    return {"hits": [doc["_source"] for doc in res["hits"]["hits"]]}



# Note: Below one is just to add 1 big data into the elastic searh
import uuid
import random
import string
import time

def generate_random_big_data(n=2500000):
    categories = ["Smartphone", "Laptop", "Tablet", "Wearable", "Camera"]
    brands = ["Apple", "Samsung", "Sony", "Dell", "HP", "Google"]

    for _ in range(n):
        product_name = f"{random.choice(brands)} {random.choice(categories)} {random.randint(1, 100)}"
        if _ == 50000:
            print( "Prajwal this is a middle value: ", product_name)
        yield {
            "_index": "products",
            "_id": str(uuid.uuid4()),
            "_source": {
                "name": product_name,
                "category": random.choice(categories),
                "price": round(random.uniform(100.0, 2000.0), 2)
            }
        }

@app.post("/addBigData")
def add_big_data():
    if not es.ping():
        return {"status": "Elasticsearch not available"}

    if es.indices.exists(index="products"):
        es.indices.delete(index="products")
    es.indices.create(index="products")

    # Insert big data (start with 10_000 for testing)
    helpers.bulk(es, generate_random_big_data())
    return {"message": "Inserted test product data into Elasticsearch"}


# Testing the number of items that are there in the elastic search...
@app.get("/countProducts")
def count_products():
    if not es.indices.exists(index="products"):
        raise HTTPException(status_code=404, detail="Index 'products' does not exist.")

    res = es.count(index="products")
    return {"total_documents": res['count']}

# 3. For Redis
import redis

# Below are the UTILITY Functions ...
# Redis client setup (connect to local Redis server)
r = redis.Redis(host="localhost", port=6379, db=0)

def acquire_ticket_lock(user_id: str, ttl_seconds: int = 30) -> bool:
    """Try to acquire lock for the user for TTL duration"""
    return r.set(f"ticket:{user_id}", "locked", ex=ttl_seconds, nx=True)   # This will send either TRUE or NONE

def is_locked(user_id: str) -> bool:
    """Check if user is locked"""
    return r.exists(f"ticket:{user_id}") == 1

def release_ticket_lock(user_id: str):
    """Forcefully release the lock (for testing)"""
    r.delete(f"ticket:{user_id}")
    
@app.post("/lock/{user_id}")
def lock_user(user_id: str):
    if acquire_ticket_lock(user_id):
        return {"status": "locked", "user_id": user_id}
    raise HTTPException(status_code=409, detail="User already has a lock")

@app.get("/status/{user_id}")
def check_lock(user_id: str):
    if is_locked(user_id):
        return {"status": "locked"}
    return {"status": "unlocked"}

@app.delete("/unlock/{user_id}")
def unlock_user(user_id: str):
    release_ticket_lock(user_id)
    return {"status": "unlocked"}
