import json
import time
import os
from kafka import KafkaProducer

def json_serializer(data):
    """Serialize Python dictionaries to JSON for Kafka."""
    return json.dumps(data).encode("utf-8")

def stream_data():
    # Connect to the local Kafka broker
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    file_path = "../webscraping/shared_data.json"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found. Run the webscraper first!")
        return

    print("Starting Kafka Producer...")
    with open(file_path, "r") as f:
        articles = json.load(f)

    # Stream each article one by one to simulate a real-time data stream
    for article in articles:
        print(f"Producing: {article.get('title', 'Unknown')}")
        # Send the message to the 'hacker-news-stream' topic
        producer.send("hacker-news-stream", article)
        # Sleep for 1 second between each message to illustrate streaming
        time.sleep(1)
    
    # Ensure all messages are sent before exiting
    producer.flush()
    print("Finished producing all messages.")

if __name__ == "__main__":
    stream_data()
