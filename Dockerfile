FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark

WORKDIR /workspace

COPY requirements.txt requirements-docker.txt ./
RUN pip install --no-cache-dir -r requirements-docker.txt

RUN mkdir -p /opt/spark/jars-extra && \
    curl -fsSL -o /opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -fsSL -o /opt/spark/jars-extra/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -fsSL -o /opt/spark/jars-extra/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars-extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars-extra/delta-storage-3.1.0.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar pyspark-shell"

COPY . .

CMD ["python", "-m", "src.compactor.compaction_daemon"]
