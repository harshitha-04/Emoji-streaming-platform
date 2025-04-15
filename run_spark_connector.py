#!/usr/bin/env python3
from spark_connector import SparkConnector
import time

def main():
    connector = SparkConnector()
    connector.start()
    
    print("Spark Connector started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Spark Connector...")

if __name__ == "__main__":
    main()
