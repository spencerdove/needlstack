FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY agent/ agent/
COPY db/ db/
COPY ingestion/__init__.py ingestion/__init__.py
COPY ingestion/feedback.py ingestion/feedback.py
COPY ingestion/feedback_classifier.py ingestion/feedback_classifier.py

# Create logs directory
RUN mkdir -p logs

EXPOSE 8000

CMD ["uvicorn", "agent.api:app", "--host", "0.0.0.0", "--port", "8000"]
