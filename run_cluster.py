#!/usr/bin/env python3
from cluster_publisher import ClusterPublisher
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_cluster.py <cluster_id>")
        sys.exit(1)
        
    cluster_id = sys.argv[1]
    cluster = ClusterPublisher(cluster_id)
    cluster.start()
    
    print(f"Cluster {cluster_id} is running. Press Ctrl+C to exit.")
    try:
        # Keep the main thread alive
        while True:
            pass
    except KeyboardInterrupt:
        print(f"\nShutting down cluster {cluster_id}...")

if __name__ == "__main__":
    main()
