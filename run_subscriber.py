#!/usr/bin/env python3
from subscriber import Subscriber
import sys
import signal
import time

def signal_handler(signum, frame):
    """Handle cleanup on signal"""
    print("\nReceived shutdown signal...")
    if 'subscriber' in globals():
        subscriber.stop()
    sys.exit(0)

def main():
    if len(sys.argv) != 3:
        print("Usage: python run_subscriber.py <subscriber_id> <cluster_id>")
        sys.exit(1)

    subscriber_id = sys.argv[1]
    cluster_id = sys.argv[2]

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        global subscriber
        subscriber = Subscriber(subscriber_id, cluster_id)
        port = subscriber.start()
        
        print(f"Subscriber {subscriber_id} started in cluster {cluster_id} on port {port}")
        print("Press Ctrl+C to stop the subscriber")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except Exception as e:
        print(f"Error running subscriber: {e}")
        if 'subscriber' in globals():
            subscriber.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
