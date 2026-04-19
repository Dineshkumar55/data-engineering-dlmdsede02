FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jdk curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    numpy==1.24.3 \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    pandas==2.0.3

RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /opt/postgresql-42.7.1.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -o /opt/spark-sql-kafka.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    -o /opt/kafka-clients.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    -o /opt/spark-token-provider.jar

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/commons-pool2.jar

ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/environment

WORKDIR /app
COPY batch_processor.py .

CMD ["/bin/bash", "-c", "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && spark-submit --jars /opt/postgresql-42.7.1.jar,/opt/spark-sql-kafka.jar,/opt/kafka-clients.jar,/opt/spark-token-provider.jar,/opt/commons-pool2.jar batch_processor.py"]
