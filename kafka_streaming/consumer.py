import json
from kafka import KafkaConsumer

def consume_data():
    print("Starting Kafka Consumer... (Waiting for messages in real-time)")
    
    # Connect to the local Kafka broker and subscribe to our topic
    consumer = KafkaConsumer(
        "hacker-news-stream",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest', # Start from the oldest available message
        group_id='news-consumers-group'
    )

    # This loop runs continuously, blocking and waiting for new messages
    try:
        for message in consumer:
            # Decode and parse the JSON message
            article = json.loads(message.value.decode("utf-8"))
            
            # Illustrate processing the consumed data
            print(f"Consumed | Rank {article.get('rank', 'N/A')} - {article.get('title', 'Unknown')}")
            print(f"           Link: {article.get('link', '')}\n")
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_data()
