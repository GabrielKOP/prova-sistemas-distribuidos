import asyncio
import json
import logging
import struct
import sys

# ANSI Escape Sequences for Colors
COLOR_RESET = "\033[0m"
COLORS = {
    "PRODUCER_NODE": "\033[93m",   # Yellow
    "PRODUCER_SERVER": "\033[95m", # Magenta
    "CONSUMER_SERVER": "\033[96m", # Cyan
    "CONSUMER_NODE": "\033[92m",   # Green
    "SYSTEM": "\033[90m",          # Grey
    "ERROR": "\033[91m",           # Red
    "INFO": "\033[97m"             # White
}

class ColoredFormatter(logging.Formatter):
    """Custom logging formatter that applies colors based on the log's component and level."""
    def format(self, record):
        component = getattr(record, "component", "SYSTEM")
        if record.levelno >= logging.WARNING:
            color = COLORS["ERROR"]
        else:
            color = COLORS.get(component, COLORS["SYSTEM"])
        
        # Format: [TIMESTAMP] [COMPONENT] LEVEL: Message
        fmt = f"{COLORS['SYSTEM']}[%(asctime)s]{color} [%(component)s] %(levelname)s: %(message)s{COLOR_RESET}"
        
        # Dynamic logger formatting
        formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")
        return formatter.format(record)

def setup_logger(name: str, component: str) -> logging.Logger:
    """Configures a logger with custom component metadata and colors."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if logger is re-initialized
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ColoredFormatter())
        logger.addHandler(handler)
        
    # Bind the component name to the logger via an Adapter
    return logging.LoggerAdapter(logger, {"component": component})

async def send_msg(writer: asyncio.StreamWriter, data: dict):
    """Sends a JSON-serializable dictionary prefixed by its 4-byte length."""
    payload = json.dumps(data).encode('utf-8')
    length_header = struct.pack('>I', len(payload))
    writer.write(length_header + payload)
    await writer.drain()

async def recv_msg(reader: asyncio.StreamReader) -> dict | None:
    """Reads a message prefixed by a 4-byte length. Returns None if connection is closed."""
    try:
        header = await reader.readexactly(4)
        length = struct.unpack('>I', header)[0]
        payload = await reader.readexactly(length)
        return json.loads(payload.decode('utf-8'))
    except (asyncio.IncompleteReadError, ConnectionResetError, ValueError):
        return None
