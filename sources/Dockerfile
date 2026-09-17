FROM python:3.11-slim

WORKDIR /app

COPY sources/requirements.txt /app/sources/requirements.txt
RUN pip install --no-cache-dir -r /app/sources/requirements.txt

COPY common /app/common
COPY sources /app/sources

# Actual command (vitals_producer.py or lab_batch_source.py) is set per
# service in docker-compose.yml so this one image serves both sources.
CMD ["python", "sources/vitals_producer.py"]
