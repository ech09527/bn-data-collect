# 币安永续 aggtrade → Kafka 采集应用
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py main.py binance_ws.py kafka_producer.py ./
COPY settings.toml ./

CMD ["python", "-u", "main.py"]
