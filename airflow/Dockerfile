FROM apache/airflow:2.9.3-python3.11

ENV AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /opt/airflow/common
COPY airflow/dags /opt/airflow/dags

CMD ["airflow", "standalone"]
