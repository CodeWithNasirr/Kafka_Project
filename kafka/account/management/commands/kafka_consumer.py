# Import necessary libraries
from django.core.management.base import BaseCommand  # BaseCommand for creating custom Django management commands
from confluent_kafka import Consumer, KafkaException, KafkaError  # Kafka consumer and exception handling from confluent_kafka
import json  # JSON library to handle message decoding
from account.models import LocationUpdate  # Importing the LocationUpdate model to save location data
# yaha per kafka_conflunt pe jo flush keya gaya ha uska data yaha pe ayega then ye uss data ko DB pe send karega
class Command(BaseCommand):
    # Command description to display in the help section of Django management
    help = 'Run Kafka Consumer to listen for location updates'

    # The 'handle' method is the entry point for the command when it is run
    def handle(self, *args, **options):
        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': 'localhost:9092',  # Kafka broker running locally
            'group.id': 'location_group',  # Consumer group ID for this consumer
            'auto.offset.reset': 'earliest'  # Start reading from the earliest available message if no offset is found
        }

        # Initialize the Kafka consumer with the given configuration
        consumer = Consumer(conf)

        # Kafka Topic: 
        # A Kafka topic is a category or feed name to which records/messages are published. Producers send data to topics, and consumers read data from topics.

        # subscribe() method:
        # This method allows the consumer to listen to one or more Kafka topics. In this case, it's subscribing to a topic called 'location_group'. Once the consumer subscribes to this topic, it will start receiving messages that are published to this topic by any Kafka producer.
        # It tells the consumer to start listening to messages that are being published to the topic named 'location_group'

        # Subscribe the consumer to the 'location_group' topic
        consumer.subscribe(['location_group'])

        try:
            # Infinite loop to keep the consumer running
            while True:
                # Poll the Kafka topic for new messages, with a timeout of 1 second
                msg = consumer.poll(timeout=1.0)

                # If no message is received, continue polling
                if msg is None:
                    continue

                # If there is an error in the message, handle it
                if msg.error():
                    # Check if the error is the "End of Partition" error (not a fatal error)
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        # Print any other error and continue
                        print(msg.error())

                # If no error, proceed to process the message

                # When messages are sent to Kafka (via a producer), they are serialized (converted to bytes). This is because Kafka works with binary data, and messages are transmitted as byte streams.
                # msg.value() returns the message's value in binary format (bytes).
                # The .decode('utf-8') part converts the binary data (bytes) into a human-readable string using UTF-8 encoding.
                
                # Decode the JSON message received from Kafka
                data = json.loads(msg.value().decode('utf-8'))

                # Save the location data to the database using the LocationUpdate model
                LocationUpdate.objects.create(
                    latitude=data['latitude'],  # Save latitude from Kafka message
                    longitude=data['longitude']  # Save longitude from Kafka message
                )

                # Print the received and saved data to the console
                print(f"Received and Saved Data: {data}")

        # Handle interruption (like Ctrl + C) to safely stop the consumer
        except KeyboardInterrupt:
            pass

        # Close the consumer when done or interrupted
        finally:
            consumer.close()
