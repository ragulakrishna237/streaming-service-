import json
import threading
import time
import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from kafka import KafkaProducer, KafkaConsumer
import uvicorn

app = FastAPI()

# A simple modern UI with two columns for Producer and Consumer
html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Kafka Stream UI</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7f6; padding: 20px; color: #333; }
            h1 { text-align: center; color: #2c3e50; }
            .header-bar { display: flex; justify-content: center; margin-bottom: 30px; }
            button { padding: 12px 24px; font-size: 16px; background-color: #3498db; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; transition: background-color 0.3s; }
            button:hover { background-color: #2980b9; }
            
            .container { display: flex; gap: 20px; max-width: 1200px; margin: 0 auto; }
            .box { flex: 1; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); height: 70vh; overflow-y: auto; }
            h2 { margin-top: 0; padding-bottom: 10px; border-bottom: 2px solid #ecf0f1; color: #34495e; }
            
            .message { padding: 12px; border-bottom: 1px solid #ecf0f1; margin-bottom: 8px; border-left: 5px solid #2ecc71; background-color: #fafafa; border-radius: 4px; animation: fadeIn 0.5s; }
            .message-prod { border-left-color: #e74c3c; }
            
            .title { font-weight: bold; font-size: 15px; margin-bottom: 5px; }
            .meta { font-size: 12px; color: #7f8c8d; }
            a { color: #3498db; text-decoration: none; }
            a:hover { text-decoration: underline; }
            
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(-10px); }
                to { opacity: 1; transform: translateY(0); }
            }
        </style>
    </head>
    <body>
        <h1>Kafka Real-Time Streaming</h1>
        <div class="header-bar">
            <button onclick="startProducer()">🚀 Start Producer Stream</button>
        </div>
        
        <div class="container">
            <div class="box">
                <h2>📦 1. Producer (Sending to Kafka)</h2>
                <div id="producer-messages"></div>
            </div>
            <div class="box">
                <h2>📥 2. Consumer (Receiving from Kafka)</h2>
                <div id="consumer-messages"></div>
            </div>
        </div>
        
        <script>
            function startProducer() {
                fetch('/start-producer', { method: 'POST' });
            }

            let pIdx = 0;
            let cIdx = 0;

            // Poll the server every 1 second for new logs
            setInterval(() => {
                fetch(`/api/logs?p_idx=${pIdx}&c_idx=${cIdx}`)
                    .then(res => res.json())
                    .then(data => {
                        // Render Producer logs
                        data.producer.forEach(item => {
                            let msg = document.createElement("div");
                            msg.className = "message message-prod";
                            msg.innerHTML = "<div class='title'>Produced: " + item.title + "</div>";
                            let container = document.getElementById("producer-messages");
                            container.insertBefore(msg, container.firstChild);
                        });
                        
                        // Render Consumer logs
                        data.consumer.forEach(item => {
                            let msg = document.createElement("div");
                            msg.className = "message";
                            msg.innerHTML = "<div class='title'>Consumed (Rank " + (item.rank || '-') + "): " + item.title + "</div><div class='meta'><a href='" + item.link + "' target='_blank'>" + item.link + "</a></div>";
                            let container = document.getElementById("consumer-messages");
                            container.insertBefore(msg, container.firstChild);
                        });
                        
                        // Update indexes so we only get new messages next time
                        pIdx = data.p_idx;
                        cIdx = data.c_idx;
                    })
                    .catch(err => console.log("Waiting for server..."));
            }, 1000);
        </script>
    </body>
</html>
"""

# Store logs in memory
producer_logs = []
consumer_logs = []

@app.get("/")
def get_ui():
    return HTMLResponse(html)

def producer_worker():
    """Runs in a background thread to produce messages"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    file_path = "../webscraping/shared_data.json"
    if not os.path.exists(file_path):
        producer_logs.append({"title": "Error: Run webscraper first!", "rank": "-", "link": "#"})
        return

    with open(file_path, "r") as f:
        articles = json.load(f)

    for article in articles:
        # Send to a fresh topic to avoid old message clashes
        producer.send("hacker-news-ui-topic", article)
        producer.flush()
        producer_logs.append(article)
        time.sleep(1.5)

@app.post("/start-producer")
def start_producer():
    # Spin up the background thread so it doesn't block the API
    t = threading.Thread(target=producer_worker)
    t.start()
    return {"status": "started"}

def consumer_worker():
    """Runs continuously in a background thread to consume messages"""
    time.sleep(1) # Wait a moment to ensure Kafka is ready
    consumer = KafkaConsumer(
        "hacker-news-ui-topic",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        group_id='ui-consumer-group-1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        consumer_logs.append(message.value)

# Start the consumer background thread immediately when the app loads
consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
consumer_thread.start()

@app.get("/api/logs")
def get_logs(p_idx: int = 0, c_idx: int = 0):
    """Returns only the new logs since the last poll"""
    return {
        "producer": producer_logs[p_idx:],
        "consumer": consumer_logs[c_idx:],
        "p_idx": len(producer_logs),
        "c_idx": len(consumer_logs)
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
