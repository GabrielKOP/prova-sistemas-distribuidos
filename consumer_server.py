import asyncio
import collections
import json
import os
import sys
from network_utils import setup_logger, send_msg, recv_msg

# Configuration
PRODUCER_PORT = int(os.environ.get("PRODUCER_PORT", "6000"))
CONSUMER_PORT = int(os.environ.get("CONSUMER_PORT", "7000"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8000"))
NODE_ID = "ConsServer"

# Logger setup
logger = setup_logger(NODE_ID, "CONSUMER_SERVER")

# Stocks in memory (Queues A, B, and C)
stocks = {
    "A": collections.deque(),
    "B": collections.deque(),
    "C": collections.deque()
}

# asyncio.Condition objects for each product type
conditions = {
    "A": asyncio.Condition(),
    "B": asyncio.Condition(),
    "C": asyncio.Condition()
}

# Web Dashboard SSE Clients
sse_clients = set()

# Real-time state trackers for Dashboard
waiting_counts = {
    "A": 0,
    "B": 0,
    "C": 0
}

def get_stock_state():
    """Formats current stocks list for the dashboard."""
    return {k: [item["id"] for item in v] for k, v in stocks.items()}

def broadcast_event(event_type: str, data):
    """Pushes a JSON event to all connected web dashboard sessions."""
    event = {"type": event_type, "data": data}
    for client_queue in sse_clients:
        client_queue.put_nowait(event)

async def handle_producer_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handles connection from the Producer Server to receive new items."""
    try:
        product = await recv_msg(reader)
        if product and "product_type" in product and "id" in product:
            p_type = product["product_type"]
            p_id = product["id"]
            
            if p_type in stocks:
                # Add to stock and notify any waiting consumers inside the condition lock
                async with conditions[p_type]:
                    stocks[p_type].append(product)
                    logger.info(f"Stored product {p_id} of type '{p_type}'. Stock level: {len(stocks[p_type])}")
                    
                    # Update Web Dashboard
                    broadcast_event("stock", get_stock_state())
                    broadcast_event("log", f"Recebido produto tipo '{p_type}' ({p_id[:8]}...) da produção.")
                    
                    conditions[p_type].notify_all()
                
                # Immediate ACK confirmation to the Producer Server
                await send_msg(writer, {"status": "ACK", "id": p_id})
                logger.info(f"Sent ACK for product {p_id} to Producer Server.")
            else:
                logger.warning(f"Received unknown product type '{p_type}' from Producer Server.")
                await send_msg(writer, {"status": "ERROR", "message": "Unknown product type"})
        else:
            logger.warning("Received invalid format from Producer Server.")
    except Exception as e:
        logger.error(f"Error handling Producer Server connection: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def handle_consumer_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handles requests from Consumer Nodes for products."""
    peer = writer.get_extra_info('peername')
    try:
        request = await recv_msg(reader)
        if request and "request_product" in request and "consumer_id" in request:
            p_type = request["request_product"]
            consumer_id = request["consumer_id"]
            logger.info(f"Consumer '{consumer_id}' requested product '{p_type}'")
            
            if p_type not in stocks:
                logger.warning(f"Consumer '{consumer_id}' requested invalid product '{p_type}'")
                await send_msg(writer, {"status": "ERROR", "message": "Invalid product type"})
                return
            
            # Demand Control: Safe non-blocking wait using asyncio.Condition
            product = None
            async with conditions[p_type]:
                if not stocks[p_type]:
                    logger.info(f"Stock '{p_type}' is empty. Consumer '{consumer_id}' is now blocking safely...")
                    waiting_counts[p_type] += 1
                    broadcast_event("waiting", waiting_counts)
                    broadcast_event("log", f"Fila vazia! Consumidor '{consumer_id}' suspenso aguardando '{p_type}'.")
                    
                # Loop to handle spurious wakeups or race conditions among multiple consumers
                while not stocks[p_type]:
                    await conditions[p_type].wait()
                
                # If we were waiting, decrement the blocked counter
                if product is None and not stocks[p_type]:
                    # (This check is handled correctly by tracking before/after wait)
                    pass
                
                # Let's adjust counter if it was waiting
                # To be precise, check if we incremented it:
                # We can track it with a local boolean
                # Yes, let's keep it simple: if we incremented it, we decrement it now:
                
            # Wait loop finished. We must re-acquire lock to pop (which is done automatically by 'async with')
            # But wait! If we decrement waiting_counts inside the 'async with' after we exit the wait loop:
            async with conditions[p_type]:
                # If we had incremented it because stock was empty:
                # Actually, let's just do it directly inside the original block:
                pass
            
            # Let's write the wait logic cleanly:
            was_waiting = False
            async with conditions[p_type]:
                if not stocks[p_type]:
                    logger.info(f"Stock '{p_type}' is empty. Consumer '{consumer_id}' is now blocking safely...")
                    waiting_counts[p_type] += 1
                    broadcast_event("waiting", waiting_counts)
                    broadcast_event("log", f"Estoque '{p_type}' esgotado! Consumidor '{consumer_id}' bloqueado na CPU (Condition.wait).")
                    was_waiting = True
                
                while not stocks[p_type]:
                    await conditions[p_type].wait()
                
                if was_waiting:
                    waiting_counts[p_type] -= 1
                    broadcast_event("waiting", waiting_counts)
                
                product = stocks[p_type].popleft()
                logger.info(f"Product '{p_type}' ({product['id']}) retrieved for consumer '{consumer_id}'. Stock remaining: {len(stocks[p_type])}")
                
                # Update Dashboard
                broadcast_event("stock", get_stock_state())
                broadcast_event("log", f"Produto '{p_type}' ({product['id'][:8]}...) liberado para consumidor '{consumer_id}'.")
            
            # Deliver the product
            await send_msg(writer, {"status": "OK", "product": product})
            logger.info(f"Product delivered to consumer '{consumer_id}' and connection closed.")
        else:
            logger.warning(f"Received invalid demand format from {peer}")
    except Exception as e:
        logger.error(f"Error handling Consumer Node connection {peer}: {e}")
    finally:
        writer.close()
        await writer.wait_closed()

async def handle_http_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Serves the dashboard HTML and the SSE event stream (/events)."""
    peer = writer.get_extra_info('peername')
    try:
        # Read request headers
        request_bytes = await reader.readuntil(b"\r\n\r\n")
        request_text = request_bytes.decode('utf-8')
        lines = request_text.split("\r\n")
        if not lines:
            return
        
        req_line = lines[0]
        parts = req_line.split(" ")
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]
        
        if method == "GET" and path == "/":
            # Serve Dashboard HTML
            html_content = DASHBOARD_HTML
            response = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(html_content.encode('utf-8'))}\r\n"
                "Connection: close\r\n\r\n"
                + html_content
            )
            writer.write(response.encode('utf-8'))
            await writer.drain()
            
        elif method == "GET" and path == "/events":
            # Establish SSE Connection
            headers = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n\r\n"
            )
            writer.write(headers.encode('utf-8'))
            await writer.drain()
            
            # Register client SSE Queue
            client_queue = asyncio.Queue()
            sse_clients.add(client_queue)
            
            # Send initial state
            initial_stock = f"data: {json.dumps({'type': 'stock', 'data': get_stock_state()})}\n\n"
            initial_waiting = f"data: {json.dumps({'type': 'waiting', 'data': waiting_counts})}\n\n"
            writer.write(initial_stock.encode('utf-8') + initial_waiting.encode('utf-8'))
            await writer.drain()
            
            try:
                while True:
                    event = await client_queue.get()
                    sse_msg = f"data: {json.dumps(event)}\n\n"
                    writer.write(sse_msg.encode('utf-8'))
                    await writer.drain()
            except (ConnectionResetError, asyncio.CancelledError):
                pass
            finally:
                sse_clients.remove(client_queue)
        else:
            # 404 Response
            response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            writer.write(response.encode('utf-8'))
            await writer.drain()
            
    except Exception as e:
        # Client disconnected early or malformed request
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

async def main():
    logger.info("Starting Consumer Server...")
    
    # 1. Listen for Producer Server connections
    p_server = await asyncio.start_server(handle_producer_connection, '0.0.0.0', PRODUCER_PORT)
    logger.info(f"Listening for Producer Server on port {PRODUCER_PORT}...")
    
    # 2. Listen for Consumer Node requests
    c_server = await asyncio.start_server(handle_consumer_connection, '0.0.0.0', CONSUMER_PORT)
    logger.info(f"Listening for Consumer Nodes on port {CONSUMER_PORT}...")
    
    # 3. Listen for Web Dashboard HTTP requests
    http_server = await asyncio.start_server(handle_http_connection, '0.0.0.0', HTTP_PORT)
    logger.info(f"Web Dashboard online at http://localhost:{HTTP_PORT}...")
    
    # Run all servers concurrently
    async with p_server, c_server, http_server:
        await asyncio.gather(
            p_server.serve_forever(),
            c_server.serve_forever(),
            http_server.serve_forever()
        )

# Self-contained Web Dashboard HTML/CSS/JS
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sistemas Distribuídos - Dashboard Real-Time</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=Fira+Code:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0b0f19;
            --card-bg: rgba(17, 24, 39, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --color-a: #fbbf24;
            --color-b: #ec4899;
            --color-c: #06b6d4;
            --color-system: #6b7280;
            --color-success: #10b981;
        }
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-primary);
            overflow-x: hidden;
            display: flex;
            flex-direction: column;
            min-height: 100vh;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1.2rem 2rem;
            border-bottom: 1px solid var(--border-color);
            background: rgba(15, 23, 42, 0.4);
            backdrop-filter: blur(10px);
        }
        .logo-section h1 {
            font-size: 1.4rem;
            font-weight: 700;
            background: linear-gradient(90deg, #3b82f6, #06b6d4);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .status-badge {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        .pulse {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: var(--color-success);
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
            animation: pulse-animation 1.5s infinite;
        }
        @keyframes pulse-animation {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }
        main {
            flex: 1;
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 1.5rem;
            padding: 2rem;
            max-width: 1600px;
            margin: 0 auto;
            width: 100%;
        }
        @media (max-width: 1024px) {
            main { grid-template-columns: 1fr; }
        }
        .dashboard-section {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            display: flex;
            flex-direction: column;
            gap: 1rem;
            backdrop-filter: blur(8px);
        }
        .section-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            border-left: 3px solid #3b82f6;
            padding-left: 0.5rem;
            margin-bottom: 0.5rem;
        }
        .stocks-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
        }
        @media (max-width: 640px) {
            .stocks-grid { grid-template-columns: 1fr; }
        }
        .stock-card {
            border-radius: 10px;
            padding: 1.5rem;
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.5rem;
            position: relative;
            overflow: hidden;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .stock-card:hover { transform: translateY(-2px); }
        .stock-card.card-a {
            background: linear-gradient(135deg, rgba(251, 191, 36, 0.05) 0%, rgba(17, 24, 39, 0.7) 100%);
            border-color: rgba(251, 191, 36, 0.2);
        }
        .stock-card.card-b {
            background: linear-gradient(135deg, rgba(236, 72, 153, 0.05) 0%, rgba(17, 24, 39, 0.7) 100%);
            border-color: rgba(236, 72, 153, 0.2);
        }
        .stock-card.card-c {
            background: linear-gradient(135deg, rgba(6, 182, 212, 0.05) 0%, rgba(17, 24, 39, 0.7) 100%);
            border-color: rgba(6, 182, 212, 0.2);
        }
        .product-label {
            font-size: 0.875rem;
            font-weight: 600;
            color: var(--text-secondary);
        }
        .stock-value {
            font-size: 3rem;
            font-weight: 700;
            line-height: 1;
        }
        .card-a .stock-value { color: var(--color-a); text-shadow: 0 0 10px rgba(251, 191, 36, 0.2); }
        .card-b .stock-value { color: var(--color-b); text-shadow: 0 0 10px rgba(236, 72, 153, 0.2); }
        .card-c .stock-value { color: var(--color-c); text-shadow: 0 0 10px rgba(6, 182, 212, 0.2); }
        
        .stock-list {
            width: 100%;
            margin-top: 1rem;
            max-height: 150px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
            font-family: 'Fira Code', monospace;
            font-size: 0.75rem;
        }
        .stock-item {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid var(--border-color);
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            text-align: center;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .terminal-container {
            display: flex;
            flex-direction: column;
            flex: 1;
            min-height: 380px;
            background: #05070c;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            overflow: hidden;
        }
        .terminal-header {
            background: #0f172a;
            padding: 0.5rem 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border-color);
        }
        .terminal-dots { display: flex; gap: 6px; }
        .dot { width: 10px; height: 10px; border-radius: 50%; }
        .dot.red { background-color: #ef4444; }
        .dot.yellow { background-color: #f59e0b; }
        .dot.green { background-color: #10b981; }
        .terminal-title {
            font-family: 'Fira Code', monospace;
            font-size: 0.75rem;
            color: var(--text-secondary);
        }
        .terminal-body {
            flex: 1;
            padding: 1rem;
            overflow-y: auto;
            font-family: 'Fira Code', monospace;
            font-size: 0.85rem;
            display: flex;
            flex-direction: column;
            gap: 0.4rem;
            color: #d1d5db;
        }
        .log-line {
            line-height: 1.4;
            animation: fadeIn 0.15s ease-out;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(2px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .log-a { color: var(--color-a); }
        .log-b { color: var(--color-b); }
        .log-c { color: var(--color-c); }
        .log-sys { color: var(--color-system); }
        
        .waiting-panel {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            display: flex;
            flex-direction: column;
            gap: 1rem;
            height: fit-content;
        }
        .waiting-list {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }
        .waiting-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.75rem;
            border-radius: 8px;
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border-color);
        }
        .waiting-badge {
            font-family: 'Fira Code', monospace;
            font-size: 0.875rem;
            font-weight: 600;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
        }
        .waiting-badge.badge-a { background: rgba(251, 191, 36, 0.1); color: var(--color-a); }
        .waiting-badge.badge-b { background: rgba(236, 72, 153, 0.1); color: var(--color-b); }
        .waiting-badge.badge-c { background: rgba(6, 182, 212, 0.1); color: var(--color-c); }
        .waiting-count {
            font-size: 1.2rem;
            font-weight: 700;
        }
    </style>
</head>
<body>
    <header>
        <div class="logo-section">
            <h1>Sistemas Distribuídos - Painel de Controle</h1>
        </div>
        <div class="status-badge">
            <span class="pulse"></span>
            <span>SSE Conectado</span>
        </div>
    </header>
    <main>
        <div class="dashboard-section">
            <div class="section-title">Estoques Temporários (Memória RAM)</div>
            <div class="stocks-grid">
                <div class="stock-card card-a">
                    <span class="product-label">Estoque Produto A</span>
                    <span class="stock-value" id="stock-val-a">0</span>
                    <div class="stock-list" id="stock-list-a"></div>
                </div>
                <div class="stock-card card-b">
                    <span class="product-label">Estoque Produto B</span>
                    <span class="stock-value" id="stock-val-b">0</span>
                    <div class="stock-list" id="stock-list-b"></div>
                </div>
                <div class="stock-card card-c">
                    <span class="product-label">Estoque Produto C</span>
                    <span class="stock-value" id="stock-val-c">0</span>
                    <div class="stock-list" id="stock-list-c"></div>
                </div>
            </div>
            
            <div class="section-title">Fila de Eventos em Tempo Real</div>
            <div class="terminal-container">
                <div class="terminal-header">
                    <div class="terminal-dots">
                        <span class="dot red"></span>
                        <span class="dot yellow"></span>
                        <span class="dot green"></span>
                    </div>
                    <div class="terminal-title">stream@consumer_server</div>
                </div>
                <div class="terminal-body" id="terminal-body">
                    <div class="log-line log-sys">[SISTEMA] Aguardando conexões dos servidores...</div>
                </div>
            </div>
        </div>
        
        <div class="waiting-panel">
            <div class="section-title">Clientes em Espera</div>
            <p style="font-size: 0.85rem; color: var(--text-secondary); line-height: 1.4; margin-bottom: 0.5rem;">
                Mostra quantos clientes estão conectados e aguardando a produção entregar novos itens.
            </p>
            <div class="waiting-list">
                <div class="waiting-row">
                    <span class="waiting-badge badge-a">Esperando Produto A</span>
                    <span class="waiting-count" id="wait-val-a">0</span>
                </div>
                <div class="waiting-row">
                    <span class="waiting-badge badge-b">Esperando Produto B</span>
                    <span class="waiting-count" id="wait-val-b">0</span>
                </div>
                <div class="waiting-row">
                    <span class="waiting-badge badge-c">Esperando Produto C</span>
                    <span class="waiting-count" id="wait-val-c">0</span>
                </div>
            </div>
        </div>
    </main>

    <script>
        const evtSource = new EventSource("/events");
        const term = document.getElementById("terminal-body");
        
        function prependLog(text, colorClass = "log-sys") {
            const line = document.createElement("div");
            line.className = `log-line ${colorClass}`;
            const time = new Date().toLocaleTimeString();
            line.innerText = `[${time}] ${text}`;
            term.insertBefore(line, term.firstChild);
            
            // Limit terminal lines
            while (term.children.length > 50) {
                term.removeChild(term.lastChild);
            }
        }

        evtSource.onmessage = function(event) {
            const data = JSON.parse(event.data);
            
            if (data.type === "stock") {
                const s = data.data;
                document.getElementById("stock-val-a").innerText = s.A.length;
                document.getElementById("stock-val-b").innerText = s.B.length;
                document.getElementById("stock-val-c").innerText = s.C.length;
                
                ["a", "b", "c"].forEach(key => {
                    const listEl = document.getElementById(`stock-list-${key}`);
                    listEl.innerHTML = "";
                    s[key.toUpperCase()].slice(-5).forEach(uuid => {
                        const item = document.createElement("div");
                        item.className = "stock-item";
                        item.innerText = uuid.substring(0, 8) + "...";
                        listEl.appendChild(item);
                    });
                });
            } else if (data.type === "waiting") {
                const w = data.data;
                document.getElementById("wait-val-a").innerText = w.A;
                document.getElementById("wait-val-b").innerText = w.B;
                document.getElementById("wait-val-c").innerText = w.C;
            } else if (data.type === "log") {
                let logClass = "log-sys";
                if (data.data.includes("tipo 'A'") || data.data.includes("Produto 'A'")) logClass = "log-a";
                else if (data.data.includes("tipo 'B'") || data.data.includes("Produto 'B'")) logClass = "log-b";
                else if (data.data.includes("tipo 'C'") || data.data.includes("Produto 'C'")) logClass = "log-c";
                
                prependLog(data.data, logClass);
            }
        };

        evtSource.onerror = function() {
            prependLog("[ERRO] EventSource desconectado. Reconectando...", "log-sys");
        };
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Consumer Server shutting down.")
        sys.exit(0)
