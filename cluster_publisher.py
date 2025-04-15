#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
import redis

class ClusterPublisher:
    def __init__(self, cluster_id, bootstrap_servers=['localhost:9092'], redis_host='localhost', redis_port=6379):
        self.cluster_id = cluster_id
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.consumer = KafkaConsumer(
            'main_emoji_topic',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def start(self):
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
    def _consume_messages(self):
        for message in self.consumer:
            subscriber_topic = f"cluster_{self.cluster_id}_emoji"
            self.producer.send(subscriber_topic, message.value)
            self.producer.flush()

    def register_subscriber(self, subscriber_id):
        """Register a new subscriber in Redis."""
        key = f"cluster_{self.cluster_id}_subscribers"
        self.redis_client.hset(key, subscriber_id, 0)  # Initialize with zero clients

    def unregister_subscriber(self, subscriber_id):
        """Unregister a subscriber from Redis."""
        key = f"cluster_{self.cluster_id}_subscribers"
        self.redis_client.hdel(key, subscriber_id)

    def assign_client_to_subscriber(self):
        """Assign clients to the least-loaded subscriber."""
        key = f"cluster_{self.cluster_id}_subscribers"
        subscribers = self.redis_client.hgetall(key)
        if not subscribers:
            return None  # No subscribers available
        # Find the subscriber with the least load
        least_loaded = min(subscribers, key=subscribers.get)
        self.redis_client.hincrby(key, least_loaded, 1)
        return least_loaded

    def release_client_from_subscriber(self, subscriber_id):
        """Decrement the client count for a subscriber."""
        key = f"cluster_{self.cluster_id}_subscribers"
        if self.redis_client.hexists(key, subscriber_id):
            self.redis_client.hincrby(key, subscriber_id, -1)

