To run this project, we are required to have three things up and running

<!-- below command will run teh docker image of the zoo keeper and the kafka. -->
1. docker-composer up -d                        docker-compose down  (to shut down)
<!-- Before below command, you are required to start the service, source venv/bin/activate -->
2. uvicorn producer.main:app --reload
<!-- Below command will show you the data in the kafka partition -->
3. python consumer/consumer.py