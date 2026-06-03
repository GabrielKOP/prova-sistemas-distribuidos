import asyncio
import os
import sys
from network_utils import setup_logger, send_msg, recv_msg

# Configuration
PORT = int(os.environ.get("PORT", "5000"))
CONSUMER_SERVER_HOST = os.environ.get("CONSUMER_SERVER_HOST", "localhost")
CONSUMER_SERVER_PORT = int(os.environ.get("CONSUMER_SERVER_PORT", "6000"))
NODE_ID = "ProdServer"

# Logger setup
logger = setup_logger(NODE_ID, "PRODUCER_SERVER")

# Internal FIFO Queue
queue = asyncio.Queue()

async def handle_producer_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handles incoming product uploads from Producer Nodes."""
    peername = writer.get_extra_info('peername')
    logger.info(f"Incoming connection from Producer Node: {peername}")
    
    try:
        data = await recv_msg(reader)
        if data and "id" in data and "product_type" in data:
            logger.info(f"Received product {data['id']} (Type: {data['product_type']}) from {data['producer_id']}")
            # Insert into the FIFO Queue
            await queue.put(data)
            logger.info(f"Product buffered. Current queue size: {queue.qsize()}")
        else:
            logger.warning(f"Received malformed data from {peername}")
    except Exception as e:
        logger.error(f"Error handling connection from {peername}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def delivery_worker():
    """Background worker that forwards buffered products to the Consumer Server
    and only dequeues them after receiving a confirmation ACK."""
    logger.info("Delivery worker started. Awaiting products in FIFO buffer...")
    while True:
        # Get product from FIFO queue
        product = await queue.get()
        logger.info(f"Pulled product {product['id']} from buffer. Attempting to deliver...")
        
        success = False
        retry_delay = 3.0
        
        while not success:
            try:
                # 1. Connect to the Consumer Server
                reader, writer = await asyncio.open_connection(
                    CONSUMER_SERVER_HOST, CONSUMER_SERVER_PORT
                )
                
                # 2. Send the product
                await send_msg(writer, product)
                logger.info(f"Sent product {product['id']} to Consumer Server. Waiting for ACK...")
                
                # 3. Wait for ACK with a timeout of 5 seconds
                try:
                    response = await asyncio.wait_for(recv_msg(reader), timeout=5.0)
                    if response and response.get("status") == "ACK" and response.get("id") == product["id"]:
                        logger.info(f"ACK received for product {product['id']}. Delivery verified!")
                        success = True
                    else:
                        logger.warning(f"Delivery verification failed: Received unexpected response {response}")
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for ACK for product {product['id']}")
                finally:
                    writer.close()
                    await writer.wait_closed()
            except Exception as e:
                logger.warning(f"Failed to connect or communicate with Consumer Server: {e}")
            
            if not success:
                logger.info(f"Retrying delivery of product {product['id']} in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                
        # Mark queue task as complete
        queue.task_done()

async def main():
    logger.info(f"Starting Producer Server listening on port {PORT}...")
    
    # Start the TCP server for Producer Nodes
    server = await asyncio.start_server(handle_producer_client, '0.0.0.0', PORT)
    
    # Start the background delivery worker
    worker_task = asyncio.create_task(delivery_worker())
    
    # Run the server
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Producer Server shutting down.")
        sys.exit(0)
