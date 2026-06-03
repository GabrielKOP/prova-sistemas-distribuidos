FROM python:3.11-slim

# Prevent Python from writing .pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1

# Force stdin, stdout, and stderr to be totally unbuffered.
# This ensures that asyncio logs appear in real-time in 'docker compose logs'.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy the requirements and python files
COPY requirements.txt .
COPY network_utils.py .
COPY producer_node.py .
COPY producer_server.py .
COPY consumer_server.py .
COPY consumer_node.py .

# Install dependencies (none are currently required, but complies with standard workflow)
RUN pip install --no-cache-dir -r requirements.txt

# Default entrypoint (will be overridden in docker-compose for each role)
CMD ["python", "producer_server.py"]
