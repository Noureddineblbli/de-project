# Start with the official Airflow image
FROM apache/airflow:2.8.1

# Switch to root user to install packages
USER root

# Install dependencies for Docker client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        software-properties-common && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - && \
    add-apt-repository \
      "deb [arch=amd64] https://download.docker.com/linux/debian \
      $(lsb_release -cs) \
      stable" && \
# Install Docker client and JDK
    apt-get update && \
    apt-get install -y --no-install-recommends \
        docker-ce-cli \
        default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to the airflow user
USER airflow

# Install PySpark and the Spark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-postgres pyspark==3.5.0
