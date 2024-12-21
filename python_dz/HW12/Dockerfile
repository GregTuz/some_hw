FROM apache/airflow:2.7.3

# Убедитесь, что выполняем команды от root
USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/

# Создание необходимых каталогов и установка прав
RUN mkdir -p /var/lib/apt/lists/partial && \
    chmod 755 /var/lib/apt/lists && \
    chmod 755 /var/lib/apt/lists/partial

# Обновление списка пакетов и установка необходимых зависимостей
RUN apt-get update && \
    apt-get install -y libpq-dev wget && \
    apt-get install -y build-essential gcc python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update

USER airflow
# Обновление pip
RUN pip install --upgrade pip

# Копирование requirements.txt и установка зависимостей
COPY requirements.txt .
RUN pip install -r requirements.txt