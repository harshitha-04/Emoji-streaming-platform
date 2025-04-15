#!/usr/bin/env python3
import requests
import threading
import time
import random
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
from collections import defaultdict

class EmojiClient:
    def __init__(self, client_id, api_url='http://localhost:5013/emoji'):
        self.client_id = client_id
        self.api_url = api_url
        self.emoji_types = ['😊', '😂', '❤️', '👍', '🎉']
        self.running = True
        self.session = None
        self.rate_limit = 100  # Target 100 emojis per second per client
        self.batch_size = 50   # Increased batch size
        self.concurrent_batches = 4  # Number of concurrent batch sends
        self.emoji_counts = defaultdict(int)
        self.current_second = int(time.time())
        
    def generate_emoji_data(self):
        """Generate random emoji data"""
        return {
            "user_id": str(uuid.uuid4()),
            "emoji_type": random.choice(self.emoji_types),
            "timestamp": datetime.now().isoformat()
        }

    def update_counts(self, count=1):
        """Update emoji counts for the current second"""
        current_time = int(time.time())
        if current_time != self.current_second:
            self.emoji_counts.clear()
            self.current_second = current_time
        self.emoji_counts[self.current_second] += count

    async def init_session(self):
        """Initialize aiohttp session with optimal settings"""
        if not self.session:
            conn = aiohttp.TCPConnector(limit=0)  # Remove connection limit
            timeout = aiohttp.ClientTimeout(total=2)
            self.session = aiohttp.ClientSession(
                connector=conn,
                timeout=timeout
            )

    async def send_emoji_batch(self):
        """Send a batch of emoji requests concurrently"""
        if not self.session:
            await self.init_session()
            
        emoji_data_batch = [self.generate_emoji_data() for _ in range(self.batch_size)]
        tasks = []
        
        for data in emoji_data_batch:
            tasks.append(self.session.post(self.api_url, json=data))
            
        try:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in responses if isinstance(r, aiohttp.ClientResponse) and r.status == 200)
            self.update_counts(success_count)
        except Exception as e:
            print(f"Batch error for client {self.client_id}: {str(e)}")

    async def run_client_async(self):
        """Run continuous stream of emoji sends with optimized rate limiting"""
        while self.running:
            try:
                start_time = time.time()
                
                # Send multiple batches concurrently
                batch_tasks = [self.send_emoji_batch() for _ in range(self.concurrent_batches)]
                await asyncio.gather(*batch_tasks)
                
                # Minimal delay to prevent overloading
                elapsed = time.time() - start_time
                if elapsed < 0.25:  # Adjusted delay
                    await asyncio.sleep(0.25 - elapsed)
                    
            except Exception as e:
                print(f"Client {self.client_id} error: {e}")
                await asyncio.sleep(0.1)

    def get_current_rate(self):
        """Get the current emission rate for this client"""
        return self.emoji_counts.get(self.current_second, 0)

    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()

class EmojiClientManager:
    def __init__(self, num_clients=10):
        self.num_clients = num_clients
        self.clients = []
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.display_task = None

    async def display_metrics(self):
        """Display metrics for all clients periodically"""
        prev_total = 0
        while True:
            current_time = int(time.time())
            print("\033[2J\033[H")  # Clear screen
            print(f"Emoji Client Metrics - {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 70)
            print("Client ID | Emojis/sec | Total Emojis | Rate Achievement % | Delta")
            print("-" * 70)
            
            total_current_rate = 0
            total_emojis = 0
            
            for client in self.clients:
                current_rate = client.get_current_rate()
                total_emojis_client = sum(client.emoji_counts.values())
                rate_achievement = (current_rate / client.rate_limit) * 100
                
                total_current_rate += current_rate
                total_emojis += total_emojis_client
                
                print(f"Client {client.client_id:2d} | {current_rate:10d} | {total_emojis_client:12d} | "
                      f"{rate_achievement:6.1f}% | {current_rate - client.rate_limit:+5d}")
            
            print("-" * 70)
            total_target = self.num_clients * 100
            total_achievement = (total_current_rate / total_target) * 100
            delta = total_current_rate - prev_total
            print(f"Total     | {total_current_rate:10d} | {total_emojis:12d} | "
                  f"{total_achievement:6.1f}% | {delta:+5d}")
            print("-" * 70)
            
            prev_total = total_current_rate
            await asyncio.sleep(1)

    async def start_clients(self):
        """Start all clients and metrics display"""
        self.clients = [EmojiClient(i) for i in range(self.num_clients)]
        self.display_task = asyncio.create_task(self.display_metrics())
        client_tasks = [client.run_client_async() for client in self.clients]
        await asyncio.gather(*client_tasks)

    async def stop_clients(self):
        """Stop all clients and metrics display"""
        if self.display_task:
            self.display_task.cancel()
            try:
                await self.display_task
            except asyncio.CancelledError:
                pass
        
        for client in self.clients:
            client.running = False
            await client.cleanup()

    def run(self):
        """Run the client manager"""
        try:
            self.loop.run_until_complete(self.start_clients())
        except KeyboardInterrupt:
            print("\nStopping clients...")
            self.loop.run_until_complete(self.stop_clients())
        finally:
            self.loop.close()

if __name__ == "__main__":
    print("Starting emoji clients...")
    manager = EmojiClientManager(num_clients=4)
    manager.run()
