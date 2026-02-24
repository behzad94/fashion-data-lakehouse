FROM apache/airflow:2.9.3-python3.8


USER root

# 1) Java is required because Spark runs on JVM
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2) Install Spark client (spark-submit)
ENV SPARK_VERSION=3.5.1
ENV HADOOP_PROFILE=hadoop3
RUN curl -fL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    | tar -xz -C /opt \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" /opt/spark

# ✅ 2.1) Add Hadoop S3A jars into the Airflow image (driver runs here)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar

# 3) Make spark-submit available in PATH
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"


# 4) Install the Airflow Spark provider (pinned)
USER airflow
RUN pip install --no-cache-dir "apache-airflow-providers-apache-spark==4.1.1"

# Great Expectations is needed because Airflow runs spark-submit in client mode,
# so the Spark driver runs inside this container and imports GE.
RUN pip install --no-cache-dir "notebook<7" "great-expectations==0.18.21" "pyyaml==6.0.2"
