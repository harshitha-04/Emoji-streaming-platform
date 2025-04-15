#!/usr/bin/env python3
from main_publisher import MainPublisher
import time

def main():
    publisher = MainPublisher()
    print("Main Publisher started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down Main Publisher...")

if __name__ == "__main__":
    main()
