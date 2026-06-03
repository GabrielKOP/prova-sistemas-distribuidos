import asyncio
import os
import random
import sys
import time
import uuid
from network_utils import setup_logger, send_msg

# Configuration from Environment Variables
PRODUCT_TYPE = os.environ.get("PRODUCT_TYPE", "A")
PRODUCER_SERVER_HOST = os.environ.get("PRODUCER_SERVER_HOST", "localhost")
PRODUCER_SERVER_PORT = int(os.environ.get("PRODUCER_SERVER_PORT", "5000"))
MIN_INTERVAL = float(os.environ.get("MIN_INTERVAL", "1.0"))
MAX_INTERVAL = float(os.environ.get("MAX_INTERVAL", "4.0"))
NODE_ID = f"ProdNode-{PRODUCT_TYPE}-{uuid.uuid4().hex[:6]}"

# Initialize Logger
logger = setup_logger(NODE_ID, "PRODUCER_NODE")

async def send_product(product: dict) -> bool:
    """Attempts to connect to the Producer Server and send the product.
    Returns True on success, False on failure."""
    try:
        reader, writer = await asyncio.open_connection(
            PRODUCER_SERVER_HOST, PRODUCER_SERVER_PORT
        )
        logger.info(f"Connected to Producer Server at {PRODUCER_SERVER_HOST}:{PRODUCER_SERVER_PORT}")
        
        logger.info(f"Sending product {product['id']} (Type: {product['product_type']})")
        await send_msg(writer, product)
        
        # Close connection cleanly
        writer.close()
        await writer.wait_closed()
        logger.info("Product sent and connection closed.")
        return True
    except Exception as e:
        logger.warning(f"Failed to send product to Producer Server: {e}. Retrying in 2 seconds...")
        return False

async def main():
    logger.info(f"Starting Producer Node for product '{PRODUCT_TYPE}' (ID: {NODE_ID})")
    
    while True:
        # 1. Sleep for a random interval before generating a product
        sleep_time = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        await asyncio.sleep(sleep_time)
        
        # 2. Generate the product payload
        product = {
            "producer_id": NODE_ID,
            "product_type": PRODUCT_TYPE,
            "id": str(uuid.uuid4()),
            "timestamp": time.time()
        }
        logger.info(f"Generated new product: {product['id']}")
        
        # 3. Keep trying to send this product until it succeeds (preventing data loss)
        success = False
        while not success:
            success = await send_product(product)
            if not success:
                await asyncio.sleep(2.0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Producer Node shutting down.")
        sys.exit(0)
