import hashlib
from flask import Flask, request, jsonify
from kafka import KafkaConsumer
import json
import threading
import socket
import sys
from werkzeug.serving import make_server

class Subscriber:
    def __init__(self, subscriber_id, cluster_id, bootstrap_servers=['localhost:9092']):
        self.subscriber_id = subscriber_id
        self.cluster_id = cluster_id
        self.clients = {}
        self.consumer = KafkaConsumer(
            f'cluster_{cluster_id}_emoji',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        self.app = Flask(__name__)
        self.server = None
        self.http_server = None

    def _get_unique_port(self, start_port=5000, max_attempts=100):
        """Find an available port starting from start_port"""
        for port in range(start_port, start_port + max_attempts):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('localhost', port))
                    return port
            except OSError:
                continue
        raise RuntimeError("Could not find an available port")

    def start(self):
        # Start the consumer thread
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

        # Set up Flask routes
        self._setup_routes()

        # Find an available port
        try:
            port = self._get_unique_port()
            print(f"Starting subscriber on port {port}")
            
            # Create a werkzeug server instead of using app.run()
            self.http_server = make_server('0.0.0.0', port, self.app)
            server_thread = threading.Thread(target=self.http_server.serve_forever)
            server_thread.daemon = True
            server_thread.start()
            
            print(f"Subscriber {self.subscriber_id} running on port {port}")
            return port
            
        except RuntimeError as e:
            print(f"Error starting subscriber: {e}")
            sys.exit(1)

    def stop(self):
        """Cleanup method to stop the subscriber"""
        if self.http_server:
            print("Shutting down HTTP server...")
            self.http_server.shutdown()
        print("Subscriber stopped")

    def _consume_messages(self):
        """Consume Kafka messages and broadcast to registered clients."""
        try:
            for message in self.consumer:
                self._broadcast_to_clients(message.value)
        except Exception as e:
            print(f"Error in consumer thread: {e}")

    def _broadcast_to_clients(self, message):
        """Send messages to all registered clients."""
        message_str = json.dumps(message) + '\n'
        message_bytes = message_str.encode('utf-8')
        
        # Create a copy of clients dict to avoid runtime modification issues
        clients_copy = self.clients.copy()
        
        for client_id, client_info in clients_copy.items():
            client_host, client_port = client_info
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)  # 5 second timeout
                    s.connect((client_host, client_port))
                    s.sendall(message_bytes)
            except Exception as e:
                print(f"Error sending to client {client_id} at {client_host}:{client_port}: {e}")
                self.clients.pop(client_id, None)  # Remove disconnected client

    def _setup_routes(self):
        @self.app.route('/register', methods=['POST'])
        def register():
            try:
                data = request.get_json()
                client_id = data.get('client_id')
                client_host = data.get('host', 'localhost')
                client_port = data.get('port')

                if not client_id or not client_port:
                    return jsonify({'error': 'Client ID and port are required'}), 400

                # Register the client
                self.clients[client_id] = (client_host, int(client_port))
                print(f"Client {client_id} registered with {client_host}:{client_port}")
                return jsonify({
                    'message': f'Client {client_id} registered successfully.',
                    'subscriber_id': self.subscriber_id,
                    'cluster_id': self.cluster_id
                }), 200
            except Exception as e:
                return jsonify({'error': f'Registration failed: {str(e)}'}), 500
