FROM apache/airflow:2.10.4-python3.10

RUN airflow db init
RUN airflow db migrate


#run airflow users create \
#    --username "user" \
#    --password "password" \
#    --firstname Grigory \
#    --lastname Tuzhba \
#    --role Admin \
#    --email gbtuzhba@edu.hse.ru \

#RUN airflow connections add 'python_final_postgresql' \
#    --conn-json '{
#        "conn_type": "postgres",
#        "login": "user",
#        "password": "password",
#        "host": "postgresql",
#        "port": 5432,
#        "schema": "public"
#    }'
#
#RUN airflow connections add 'python_final_mysql' \
#    --conn-json '{
#        "conn_type": "mysql",
#        "login": "user",
#        "password": "password",
#        "host": "mysql",
#        "port": 3306,
#        "schema": "public"
#    }'
#
#RUN airflow connections add 'python_final_spark' \
#    --conn-json '{
#        "conn_type": "generic",
#        "host": "spark://spark-master",
#        "port": "7077",
#        "extra": {
#            "deploy-mode": "client",
#            "spark_binary": "spark3-submit"
#        }
#    }'

CMD ["webserver"]