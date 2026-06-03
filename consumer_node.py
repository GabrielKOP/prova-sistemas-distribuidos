import asyncio
import os
import random
import sys
import time
import uuid
from network_utils import setup_logger, send_msg, recv_msg

# Configuration
CONSUMER_SERVER_HOST = os.environ.get("CONSUMER_SERVER_HOST", "localhost")
CONSUMER_SERVER_PORT = int(os.environ.get("CONSUMER_SERVER_PORT", "7000"))
MIN_INTERVAL = float(os.environ.get("MIN_INTERVAL", "2.0"))
MAX_INTERVAL = float(os.environ.get("MAX_INTERVAL", "6.0"))
NODE_ID = os.environ.get("NODE_ID", f"ConsNode-{uuid.uuid4().hex[:6]}")

# Logger setup
logger = setup_logger(NODE_ID, "CONSUMER_NODE")

async def request_product(p_type: str) -> dict | None:
    """Connects to the Consumer Server, requests a specific product type,
    and returns the received product or None on failure."""
    try:
        reader, writer = await asyncio.open_connection(
            CONSUMER_SERVER_HOST, CONSUMER_SERVER_PORT
        )
        
        request = {
            "consumer_id": NODE_ID,
            "request_product": p_type
        }
        
        # Send product request
        await send_msg(writer, request)
        
        # Await the response (wait is passive and handled by the server using conditions)
        start_time = time.time()
        response = await recv_msg(reader)
        elapsed = time.time() - start_time
        
        writer.close()
        await writer.wait_closed()
        
        if response and response.get("status") == "OK":
            product = response.get("product")
            logger.info(f"Successfully consumed product '{p_type}' ({product['id']}) after waiting {elapsed:.2f}s!")
            return product
        else:
            logger.warning(f"Failed to receive product: {response}")
            return None
    except Exception as e:
        logger.warning(f"Connection error to Consumer Server: {e}")
        return None

async def main():
    logger.info(f"Starting Consumer Node {NODE_ID}...")
    
    while True:
        # 1. Randomly pick a product type A, B, or C
        target_product = random.choice(["A", "B", "C"])
        logger.info(f"Decided to request product '{target_product}'")
        
        # 2. Request from Consumer Server and block until fulfilled
        success = False
        while not success:
            product = await request_product(target_product)
            if product:
                success = True
            else:
                logger.info("Retrying request in 3 seconds...")
                await asyncio.sleep(3.0)
                
        # 3. Sleep for a random interval before requesting again
        sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        logger.info(f"Sleeping for {sleep_time:.2f}s before starting new consumption cycle.")
        await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Consumer Node shutting down.")
        sys.exit(0)
