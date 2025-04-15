#!/usr/bin/env python3
from kafka import KafkaProducer
import json

class MainPublisher:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def publish_message(self, topic, message):
        """
        Publishes aggregated emoji data to specified topic
        message format: {
            'start': timestamp,
            'end': timestamp,
            'emoji_type': str,
            'scaled_count': int,
            'aggregation_ratio': int
        }
        """
        self.producer.send(topic, message)
        self.producer.flush()
