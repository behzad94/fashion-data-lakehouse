FROM spark:3.5.1

# The base image often sets USER to non-root.
# We must switch to root to change permissions.
USER root

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/

# Make jars readable by everyone (Spark runs as non-root)
RUN chmod 644 /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar

RUN pip3 install pyspark pytest

# Go back to the default user (safe). If the base image uses another user, this still works:
# world-readable jars = no permission problem.
USER 185
