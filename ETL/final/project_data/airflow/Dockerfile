FROM apache/airflow:2.9.1

USER root

RUN rm -f /etc/apt/sources.list.d/mssql-release.list && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update --allow-releaseinfo-change && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/

RUN mkdir -p /var/lib/apt/lists/partial && \
    chmod 755 /var/lib/apt/lists && \
    chmod 755 /var/lib/apt/lists/partial

RUN apt-get update && \
    apt-get install -y libpq-dev wget && \
    apt-get install -y build-essential gcc python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
