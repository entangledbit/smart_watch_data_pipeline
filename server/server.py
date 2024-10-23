import aiohttp
import asyncio
from aiohttp import web
import json
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError
import argparse
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime

# Kafka configurations
KAFKA_TOPIC = 'events_topic'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Async Kafka Producer
async def create_kafka_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        linger_ms=10,
        compression_type="gzip"  # Use gzip if lz4 isn't working
    )
    await producer.start()
    return producer

# Retry logic for Kafka failures
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def send_to_kafka(producer, event):
    try:
        await producer.send_and_wait(KAFKA_TOPIC, event)
        logger.info(f"Successfully sent event: {event}")
    except KafkaTimeoutError:
        logger.error(f"Failed to send event to Kafka: {e}")
        raise
    except KafkaError as e:
        logger.error(f"Failed to send event to Kafka: {e}")
        raise

# HTTP server handling real-time streaming events
async def handle_stream(request):
    producer = request.app['kafka_producer']
    try:
        # Streaming data from client
        async for msg in request.content:
            event_data = json.loads(msg.decode('utf-8'))
            logger.info(f"Received event: {event_data}")
            await send_to_kafka(producer, event_data)  # Send to Kafka
        return web.Response(status=200, text="Stream received")
    except Exception as e:
        logger.error(f"Error processing stream: {e}")
        return web.Response(status=500, text="Internal Server Error")

# Graceful shutdown for AIOKafkaProducer
async def close_kafka_producer(app):
    producer = app['kafka_producer']
    await producer.stop()
    logger.info("Kafka producer closed.")

# App factory to setup routes and Kafka producer
async def create_app():
    app = web.Application()
    app['kafka_producer'] = await create_kafka_producer()

    # Register cleanup to close Kafka producer on shutdown
    app.on_cleanup.append(close_kafka_producer)

    # Add routes
    app.router.add_post('/stream', handle_stream)
    
    return app

# Main entry point to start the server
async def init_app(args):
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=args.host, port=args.port)
    await site.start()

    logger.info(f"Server started on {args.host}:{args.port}")
    try:
        # Keep the application running
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour and repeat
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Real-time event streaming server.")
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to run the server on.')
    parser.add_argument('--port', type=int, default=62333, help='Port to run the server on.')

    args = parser.parse_args()

    # Use asyncio.run() to manage the event loop and server
    try:
        asyncio.run(init_app(args))
    except KeyboardInterrupt:
        logger.info("Server stopped manually.")
