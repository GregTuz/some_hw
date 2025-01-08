FROM apache/airflow:2.10.4-python3.10

COPY airflow_setup.sh /opt/setup_scripts/airflow_setup.sh

USER root
RUN chmod +x /opt/setup_scripts/airflow_setup.sh
RUN mkdir -p /usr/local/airflow/db /opt/airflow/dags /opt/airflow/scripts
RUN chown "50000:0" /opt/setup_scripts/airflow_setup.sh
RUN chown "50000:0" /usr/local/airflow/db
RUN chown "50000:0" /opt/airflow/dags
RUN chown "50000:0" /opt/airflow/scripts

USER airflow

CMD ["bash", "-c", "/opt/setup_scripts/airflow_setup.sh"]