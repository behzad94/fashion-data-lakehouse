FROM spark:3.5.1

USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262


RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-distutils python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Make python/pip point to 3.11
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Force Spark to use python3.11 on BOTH driver/executors (important)
ENV PYSPARK_PYTHON=/usr/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
# -----------------------------------------------

# Add Hadoop AWS jars so Spark can talk to MinIO (S3A)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar

# Install Python packages used in Spark jobs
RUN pip install --no-cache-dir pytest
RUN pip install --no-cache-dir "notebook<7" "great-expectations==0.18.21" "pyyaml==6.0.2"

USER 185
