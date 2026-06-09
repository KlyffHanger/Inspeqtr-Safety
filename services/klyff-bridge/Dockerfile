FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY klyff_orchestrator.py .
COPY klyff_bridge ./klyff_bridge

CMD ["python", "app.py"]
