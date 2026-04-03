FROM python:3.11-slim

WORKDIR /app

# Copy requirements and streams processor
COPY streams-processor/requirements.txt .
COPY streams-processor/stream_processor.py .
COPY schemas/ /app/schemas/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run streams processor
CMD ["python", "stream_processor.py"]
