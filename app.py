#!/usr/bin/env python3
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from datetime import datetime

app = Flask(__name__)

# Configure Kafka Producer with 500ms flush interval
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=16384,  # 16KB batch size
    linger_ms=500,     # 500ms flush interval
    acks='all'
)

@app.route('/emoji', methods=['POST'])
def receive_emoji():
    """
    Endpoint to receive emoji data and send to Kafka queue
    Expected JSON format:
    {
        "user_id": "user123",
        "emoji_type": "😊",
        "timestamp": "2024-01-01T12:00:00"
    }
    """
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['user_id', 'emoji_type', 'timestamp']
        if not all(field in data for field in required_fields):
            return jsonify({"error": "Missing required fields"}), 400

        # Send to Kafka topic asynchronously
        future = producer.send('emoji_topic', value=data)
        
        # Optional: Wait for the message to be sent
        # future.get(timeout=2)
        
        return jsonify({
            "status": "success",
            "message": "Emoji data queued successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "error": str(e),
            "message": "Failed to process emoji data"
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5013, debug=True)
