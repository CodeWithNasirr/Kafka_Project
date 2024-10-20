# .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
# .\bin\windows\kafka-server-start.bat .\config\server.properties

# Import necessary libraries
from confluent_kafka import Producer  # Kafka Producer from confluent_kafka library
import os
import json
import time


# Here the kafka main logic is firslty u have to configurations  the kafka_producer with the kafka_broker details and then initialize the Producer with the configurations and then  fetch your data_message logic in the mid sections and # Produce the message to the Kafka topic as a JSON-encoded string ex(producer.produce('topic_name', key='key', value='message', callback=delevery_report)) and then make a # Callback function to handle delivery reports
# It will print success or failure of message delivery and lastly # Flush the producer to make sure the message is sent immediately





# Configuration for Kafka producer with the Kafka broker details
conf = {'bootstrap.servers': 'localhost:9092'}  # Kafka server running on localhost:9092
producer = Producer(**conf)  # Initialize the Producer with the configuration

# Define start and end latitude and longitude values
# Starting location (example coordinates)
start_latitude = 19.760
start_longitude = 72.8777
# Ending location (example destination coordinates)
end_latitude = 18.5209
end_longitude = 73.8439

# Define the number of steps for simulating movement
num_steps = 1000  # Number of steps between start and end
# Calculate the incremental step size for latitude and longitude
step_size_latitude = (end_latitude - start_latitude) / num_steps
step_size_longitude = (end_longitude - start_longitude) / num_steps
currents_step = 0  # Initialize current step count

# Callback function to handle delivery reports
# It will print success or failure of message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message Delivery Failed: {err}")  # If there's an error, print the error
    else:
        print(f"Message Delivered to {msg.topic()} [Partition: {msg.partition()}]")  # On success, print topic and partition

# Define the Kafka topic where messages will be sent
topic = 'location_group'

# Infinite loop to simulate continuous movement and message production
while True:
    # Calculate the current latitude and longitude based on the current step
    latitude = step_size_latitude + start_latitude * currents_step
    longitude = step_size_longitude + start_longitude * currents_step
    # Create a dictionary with the calculated latitude and longitude
    data = {
        'latitude': latitude,
        'longitude': longitude
    }
    
    # Print the data being sent to Kafka
    print(data)

    # Produce the message to the Kafka topic as a JSON-encoded string
    producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)

    # Flush the producer to make sure the message is sent immediately
    producer.flush()

    # Increment the step count to simulate movement
    currents_step += 1

    # If the current step exceeds the number of steps, reset to 0 (to simulate continuous movement)
    if currents_step > num_steps:
        currents_step = 0

    # Sleep for 2 seconds between messages to simulate real-time updates
    time.sleep(2)
