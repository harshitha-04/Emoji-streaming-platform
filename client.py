import requests
import socket
import threading
import json
import time

class EmojiClient:
    def __init__(self, client_id, subscriber_host, subscriber_port, client_port):
        self.client_id = client_id
        self.subscriber_host = subscriber_host
        self.subscriber_port = subscriber_port
        self.client_port = client_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.running = True
        self.connected_to_subscriber = False

    def register_with_subscriber(self):
        registration_url = f"http://{self.subscriber_host}:{self.subscriber_port}/register"
        payload = {
            "client_id": self.client_id,
            "host": "localhost",  # Using localhost for local testing
            "port": self.client_port
        }
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries and not self.connected_to_subscriber:
            try:
                response = requests.post(registration_url, json=payload)
                if response.status_code == 200:
                    print(f"Successfully registered with Subscriber {self.subscriber_host}:{self.subscriber_port}")
                    self.connected_to_subscriber = True
                else:
                    print(f"Registration failed: {response.text}")
                    retry_count += 1
                    time.sleep(2)  # Wait before retrying
            except Exception as e:
                print(f"Error registering with subscriber: {e}")
                retry_count += 1
                time.sleep(2)  # Wait before retrying
        
        if not self.connected_to_subscriber:
            raise ConnectionError("Failed to register with subscriber after maximum retries")

    def start_receiving(self):
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind(('0.0.0.0', self.client_port))
            self.socket.listen(5)
            print(f"Client listening on port {self.client_port}")
            self.receiver_thread = threading.Thread(target=self._accept_connections, daemon=True)
            self.receiver_thread.start()
        except Exception as e:
            print(f"Error starting receiver: {e}")
            self.close()
            raise

    def _accept_connections(self):
        while self.running:
            try:
                conn, addr = self.socket.accept()
                print(f"Accepted connection from {addr}")
                threading.Thread(target=self._receive_messages, args=(conn,), daemon=True).start()
            except Exception as e:
                if self.running:  # Only print error if we're still supposed to be running
                    print(f"Error accepting connection: {e}")

    def _receive_messages(self, conn):
        buffer = ""
        while self.running:
            try:
                data = conn.recv(4096).decode('utf-8')
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    message = json.loads(line)
                    self._display_emoji(message)
            except json.JSONDecodeError as e:
                print(f"Error decoding message: {e}")
            except Exception as e:
                print(f"Error receiving message: {e}")
                break
        conn.close()

    def _display_emoji(self, message):
        print(f"Received Emoji Update: {message}")

    def send_emoji(self, emoji):
        """Send emoji to the subscriber (Note: This is just for testing, actual emoji sending happens in client_simulator.py)"""
        try:
            print(f"Emoji '{emoji}' sent to subscriber")
        except Exception as e:
            print(f"Error sending emoji: {e}")

    def close(self):
        """Cleanup method to properly close the client"""
        print("Closing client connection...")
        self.running = False
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except:
            pass  # Socket might already be closed
        self.socket.close()
        print("Client connection closed")
