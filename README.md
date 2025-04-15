# Emoji Streaming Platform

A distributed system for processing and distributing emoji data in real-time using Kafka, Spark Streaming, and Flask.

## System Architecture


The system consists of several components:
1. **Emoji Producer**: Flask API that receives emoji data
2. **Client Simulator**: Generates emoji data for testing
3. **Spark Streaming Processor**: Aggregates emoji data
4. **Kafka Message Bus**: Handles message passing between components
5. **Cluster Publisher**: Distributes data to subscribers
6. **Subscribers**: Deliver data to end clients

## Components

### Core Services
- `app.py`: Flask API endpoint for receiving emoji data
- `client_simulator.py`: Simulates multiple clients sending emoji data
- `spark_streaming_consumer.py`: Spark job that processes emoji streams
- `spark_connector.py`: Bridges Spark output to main Kafka topic

### Distribution Layer
- `main_publisher.py`: Publishes aggregated data to main topic
- `cluster_publisher.py`: Distributes data to cluster subscribers
- `subscriber.py`: Handles client connections and message delivery
- `client.py`: End-user client that receives emoji updates

## Prerequisites

- Python 3.7+
- Apache Kafka
- Apache Spark 3.1+
- Redis (for cluster management)
- Flask

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/harshitha-04/Emoji-streaming-platform.git
   cd Emoji-streaming-platform
   ```

2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up Kafka and create the required topics:
   ```bash
   kafka-topics --create --topic emoji_topic --bootstrap-server localhost:9092
   kafka-topics --create --topic main_emoji_topic --bootstrap-server localhost:9092
   ```

## Running the System

1. Start the emoji API:
   ```bash
   python app.py
   ```

2. Run the Spark streaming processor:
   ```bash
   python spark_streaming_consumer.py
   ```

3. Start the main publisher:
   ```bash
   python run_main_publisher.py
   ```

4. Launch a cluster publisher:
   ```bash
   python run_cluster.py cluster1
   ```

5. Start a subscriber:
   ```bash
   python run_subscriber.py subscriber1 cluster1
   ```

6. Run client simulator for testing:
   ```bash
   python client_simulator.py
   ```

7. Connect a client:
   ```bash
   python run_client.py
   ```

## Configuration

Key configuration files:
- `app.py`: Kafka producer settings
- `spark_streaming_consumer.py`: Spark processing parameters
- `cluster_publisher.py`: Redis connection settings

## API Endpoints

### Emoji API (`app.py`)
- `POST /emoji`: Receive emoji data
  ```json
  {
    "user_id": "user123",
    "emoji_type": "😊",
    "timestamp": "2024-01-01T12:00:00"
  }
  ```

### Subscriber API
- `POST /register`: Register a new client
  ```json
  {
    "client_id": "client1",
    "host": "localhost",
    "port": 6000
  }
  ```

## Monitoring

The client simulator includes a real-time metrics display showing:
- Emojis processed per second
- Rate achievement percentage
- Total emojis processed
- Per-client statistics

## Scaling

The system is designed to scale horizontally:
- Multiple cluster publishers can handle different subscriber groups
- Subscribers can be added dynamically
- Spark processing can be scaled by adding worker nodes

## Troubleshooting

Common issues:
1. **Kafka connection problems**: Verify Kafka is running and accessible
2. **Port conflicts**: Ensure unique ports for each subscriber/client
3. **Spark errors**: Check Spark logs in `/tmp/spark-events`
