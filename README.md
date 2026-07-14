# Streaming Service Project

This repository is a standalone project focused on real-time data streaming using **Apache Kafka**, combined with **Web Scraping** to generate the data stream, and a **FastAPI UI** to visualize the streaming process.

## Project Structure

The project is divided into two main components:

### 1. Web Scraping (`/webscraping`)
This component is responsible for gathering the initial dataset that will be streamed.
- `load_sanbox_data.py`: A Scrapy spider that scrapes the front page of Hacker News (titles, links, and ranks).
- `shared_data.json`: The output JSON file where the scraped data is persisted.
- `Dockerfile` & `compose.yaml`: Docker configuration for running the scraper in an isolated environment.

### 2. Kafka Streaming & Visualization (`/kafka_streaming`)
This component handles the real-time streaming of the scraped data and provides a web UI to visualize the producer and consumer in action.
- `producer.py`: Reads the `shared_data.json` file and streams each article one by one to a Kafka topic (`hacker-news-stream`).
- `consumer.py`: A basic Kafka consumer script.
- `ui_app.py`: A FastAPI application that serves a modern HTML UI. It runs a background producer and consumer, streaming data to a topic (`hacker-news-ui-topic`) and visualizing the messages in real-time on a web dashboard.
- `compose.yaml`: Docker Compose configuration to easily spin up a local Kafka broker and Zookeeper instance.

## Setup and Installation

### Prerequisites
- Python 3.8+
- Docker and Docker Compose (to run the Kafka broker)

### 1. Start the Kafka Broker
Navigate to the `kafka_streaming` directory and start the Kafka environment using Docker Compose:
```bash
cd kafka_streaming
docker-compose up -d
```

### 2. Install Python Dependencies
Install the required packages for both components:
```bash
pip install -r webscraping/requirements.txt
pip install -r kafka_streaming/requirements.txt
```

### 3. Generate the Data (Web Scraping)
Run the web scraper to generate the `shared_data.json` file:
```bash
cd webscraping
python load_sanbox_data.py
cd ..
```

### 4. Run the Streaming UI
Start the FastAPI application to visualize the streaming:
```bash
cd kafka_streaming
python ui_app.py
```
Then, open your browser and navigate to `http://127.0.0.1:8001`. Click the "Start Producer Stream" button to watch the data flow from the producer to the consumer in real-time!
