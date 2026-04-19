FROM bitnami/spark:3.5.0
USER root
RUN pip install --no-cache-dir kafka-python==2.0.2 psycopg2-binary==2.9.9 pandas==2.1.0
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /opt/bitnami/spark/jars/postgresql-42.7.1.jar
WORKDIR /app
COPY batch_processor.py .
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--jars", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar", \
     "batch_processor.py"]
