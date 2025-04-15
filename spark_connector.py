#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
from main_publisher import MainPublisher
import threading

class SparkConnector:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            'emoji_topic',  # Topic where Spark writes aggregated data
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.main_publisher = MainPublisher(bootstrap_servers)
        
    def start(self):
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
    def _consume_messages(self):
        for message in self.consumer:
            # Forward aggregated emoji data to main_emoji_topic
            self.main_publisher.publish_message('main_emoji_topic', message.value)
