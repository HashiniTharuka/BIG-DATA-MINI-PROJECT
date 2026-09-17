# Official Apache Spark image (Bitnami's bitnami/spark tags were removed
# from Docker Hub's free tier in 2025). This base already includes Python
# 3.8 + pip and the bundled PySpark under $SPARK_HOME/python, so no extra
# apt-get install is needed.
FROM apache/spark:3.5.1

USER root
WORKDIR /app

COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip3 install --no-cache-dir -r /app/streaming/requirements.txt

COPY common /app/common
COPY streaming /app/streaming

# Pre-create the Ivy cache dir with correct ownership so the named volume
# mounted here at runtime inherits writable permissions for the spark user.
RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /app /home/spark/.ivy2
USER spark

ENV PYTHONPATH=/app

# First startup needs internet access once to fetch the Kafka + Postgres
# connector jars from Maven Central (cached in the ivy2 volume afterwards).
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/streaming/vitals_stream_processor.py"]
