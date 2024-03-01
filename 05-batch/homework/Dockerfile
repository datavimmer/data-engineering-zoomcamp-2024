# Use OpenJDK 11 as the base image
FROM openjdk:11-jdk

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/spark

RUN apt-get update && \
    apt-get install -y wget && \
    mkdir -p "${SPARK_HOME}" && \
    wget --no-verbose "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1 && \
    rm /tmp/spark.tgz

# Expose the Spark UI port
# EXPOSE 4040

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Jupyter and findspark
RUN pip3 install jupyter findspark

RUN mkdir /workspace

# Create a non-root user with sudo access
ARG username=jovyan
RUN useradd -m -s /bin/bash -N -u 1000 $username && \
    chmod -R a+w /workspace && \
    chown -R $username /workspace

USER $username


# Expose the Jupyter port
EXPOSE 8888

# Start Jupyter Notebook
WORKDIR /workspace
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
