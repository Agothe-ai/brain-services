FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5555 5558 5562 5564 5575
CMD ["python", "memory_bridge.py"]
