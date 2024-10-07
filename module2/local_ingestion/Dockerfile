FROM python:3.9

WORKDIR /app

COPY scripts/ingest_script.py .


RUN pip install --no-cache-dir pandas pyarrow psycopg2-binary sqlalchemy

RUN mkdir -p /app/data && \
    chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} /app/data

CMD ["python", "ingest_script.py"]