FROM apache/airflow:2.10.4-python3.10

RUN airflow db init
RUN airflow db migrate

USER root

# Очистка всех источников репозиториев, которые могут вызывать проблемы
RUN rm -f /etc/apt/sources.list.d/microsoft-prod.list && \
    rm -f /etc/apt/sources.list.d/* && \
    echo "deb http://deb.debian.org/debian/ bookworm main" > /etc/apt/sources.list && \
    apt-get clean

# Устанавливаем только необходимые пакеты
RUN apt-get update && \
    apt-get install -y ca-certificates openjdk-17-jdk && \
    apt-get clean
#
## Копируем requirements.txt и устанавливаем Python зависимости
#COPY ./requirements.txt /opt/airflow/requirements.txt
#RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

RUN mkdir -p /opt/airflow/spark/jars

RUN curl -o /opt/airflow/spark/jars/postgresql-42.2.18.jar https://jdbc.postgresql.org/download/postgresql-42.2.18.jar && \
    curl -o /opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar

USER airflow
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"

# Entrypoint
CMD ["scheduler"]