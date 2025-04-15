#!/usr/bin/env python3
from client import EmojiClient
import time
import sys

def main():
    try:
        # Provide the necessary arguments for EmojiClient
        client_id = input("Enter client ID: ")  # e.g., "client1"
        subscriber_host = "localhost"  # The host of the subscriber
        
        while True:
            try:
                subscriber_port = int(input("Enter subscriber port: "))  # e.g., 5001
                client_port = int(input("Enter client port: "))  # e.g., 6000
                break
            except ValueError:
                print("Please enter valid port numbers")
        
        # Initialize the EmojiClient with the provided arguments
        client = EmojiClient(client_id, subscriber_host, subscriber_port, client_port)
        
        # Start the client
        try:
            # Start receiving messages from the subscriber
            client.start_receiving()
            
            # Register with the subscriber
            client.register_with_subscriber()
            
            print(f"Client {client_id} started. Enter emojis to send (press Ctrl+C to exit)")
            
            # Main input loop
            while True:
                emoji = input("Enter an emoji: ")
                if emoji.strip():  # Only send non-empty emojis
                    client.send_emoji(emoji)
                
        except ConnectionError as e:
            print(f"Connection error: {e}")
        except Exception as e:
            print(f"Error during client operation: {e}")
        
    except KeyboardInterrupt:
        print("\nShutting down client...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()
