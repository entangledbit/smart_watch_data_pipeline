import json
import asyncio
import aiohttp
from datetime import datetime
import argparse


class StreamingClient:
    def __init__(self, json_file, server_url, speed_factor, fast_mode):
        self.server_url = server_url
        self.speed_factor = speed_factor
        self.fast_mode = fast_mode
        self.events_by_device = self.group_events_by_device(json_file)
        self.total_events = sum(len(events) for events in self.events_by_device.values())  # Total number of events
        self.number_of_events_sent = 0

    def group_events_by_device(self, json_file):
        """Load and group events by device_id or 'Device ID'."""
        with open(json_file, 'r') as file:
            events = json.load(file)

        grouped_events = {}
        for event in events:
            device_id = event.get('device_id') or event.get('Device ID')
            if device_id:
                if device_id not in grouped_events:
                    grouped_events[device_id] = []
                grouped_events[device_id].append(event)

        # Sort events for each device by timestamp or date
        for device_id, event_list in grouped_events.items():
            grouped_events[device_id] = sorted(event_list, key=self.get_event_timestamp)

        return grouped_events

    def get_event_timestamp(self, event):
        """Extract timestamp as float from either 'timestamp' or 'date' field."""
        if 'timestamp' in event and isinstance(event['timestamp'], float):
            return event['timestamp']
        elif 'date' in event and isinstance(event['date'], str):
            event_date = datetime.strptime(event['date'], '%Y-%m-%d %H:%M:%S.%f')
            return event_date.timestamp()
        return 0  # Default if no timestamp is found

    async def send_event(self, session, device_id, event):
        """Send an event via HTTP using aiohttp."""
        async with session.post(self.server_url, json=event) as response:
            if response.status == 200:
                self.number_of_events_sent += 1
                print(f"Device: {device_id} | Event sent ({self.number_of_events_sent}/{self.total_events})")
            else:
                print(f"Device: {device_id} | Failed to send event with status {response.status}")

    async def process_device_events(self, session, device_id, events):
        """Process events for a device sequentially."""
        previous_timestamp = self.get_event_timestamp(events[0])

        for event in events:
            current_timestamp = self.get_event_timestamp(event)

            # Calculate the delay based on the time difference between consecutive events
            if not self.fast_mode:
                delay = (current_timestamp - previous_timestamp) / self.speed_factor
                await asyncio.sleep(delay)  # Sleep asynchronously for the given delay

            await self.send_event(session, device_id, event)

            # Update previous timestamp to the current one
            previous_timestamp = current_timestamp

    async def start_streaming(self):
        """Start streaming events for all devices."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for device_id, events in self.events_by_device.items():
                task = asyncio.create_task(self.process_device_events(session, device_id, events))
                tasks.append(task)

            # Wait for all device event processing to finish
            await asyncio.gather(*tasks)
        print("All events sent successfully.")


async def main(json_file, server_url, speed_factor, fast_mode):
    client = StreamingClient(json_file, server_url, speed_factor, fast_mode)
    await client.start_streaming()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stream smartwatch data to a server.")
    parser.add_argument('json_file', type=str, help='Path to the JSON file with event data.')
    parser.add_argument('--server_url', type=str, default="http://localhost:62333/stream", help='URL of the server to send events to.')
    parser.add_argument('--speed', type=float, default=1.0, help='Speed factor for the simulation (e.g., 1.0 for real-time, 10.0 for 10x faster).')
    parser.add_argument('--fast_mode', action='store_true', help='Send events as fast as possible without delay.')

    args = parser.parse_args()

    # Run the asyncio event loop
    asyncio.run(main(args.json_file, args.server_url, args.speed, args.fast_mode))
