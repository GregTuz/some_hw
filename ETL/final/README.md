# ETL Final Project

## Описание проекта

Проект представляет собой систему ETL (Extract, Transform, Load), которая переносит данные из MongoDB в PostgreSQL. Процесс автоматизирован с использованием Apache Airflow и Spark. Данные генерируются в MongoDB, предварительно обрабатываются и загружаются в PostgreSQL для дальнейшего анализа.

---

## Из чего состоит проект

1. **MongoDB**:
   - База данных: `ETL_MONGO_FINAL`
   - Пользователь: `${MONGO_INITDB_ROOT_USERNAME}` (администратор)
   - Пароль: `${MONGO_INITDB_ROOT_PASSWORD}`
   - Интерфейс управления: [Mongo Express](http://localhost:8081)

2. **PostgreSQL**:
   - База данных: `etl_data` (основная) и `airflow_meta` (для метаданных Airflow)
   - Пользователь: `${POSTGRES_USER}` (администратор)
   - Пароль: `${POSTGRES_PASSWORD}`
   - Интерфейс управления: [PgAdmin](http://localhost:6060)
     - Логин: `${PGADMIN_DEFAULT_EMAIL}`
     - Пароль: `${PGADMIN_DEFAULT_PASSWORD}`

3. **Airflow**:
   - Веб-интерфейс: [Airflow Webserver](http://localhost:8080)
     - Логин: `admin`
     - Пароль: `admin`
   - DAGs:
     - Генерация данных в MongoDB
     - Перенос данных из MongoDB в PostgreSQL

4. **Spark**:
   - Используется для преобразования данных между MongoDB и PostgreSQL.
   - Подключается через JDBC-коннекторы (PostgreSQL и MongoDB).

---

## Как запустить проект локально

### Предварительные требования

1. Установите Docker и Docker Compose.
2. Файл `.env` с переменными окружения уже находится в корневой директории проекта. Убедитесь, что он содержит следующие переменные:

   ```env
   # Mongo
   MONGO_INITDB_ROOT_USERNAME=mongo_user
   MONGO_INITDB_ROOT_PASSWORD=mongo_password
   MONGO_READER_USERNAME=reader
   MONGO_READER_PASSWORD=reader_password
   MONGO_DB_NAME=ETL_MONGO_FINAL

   # Mongo_Ui
   ME_CONFIG_MONGODB_ADMINUSERNAME=mongo_user
   ME_CONFIG_MONGODB_ADMINPASSWORD=mongo_password
   ME_CONFIG_MONGODB_URL=mongodb://mongo_user:mongo_password@mongo:27017/ETL_MONGO_FINAL

   # PgAdmin
   PGADMIN_DEFAULT_EMAIL=greg@gmail.com
   PGADMIN_DEFAULT_PASSWORD=pgadmin_password

   # Postgres
   POSTGRES_USER=postgres_user
   POSTGRES_PASSWORD=postgres_password

   # AirFlow
   AIRFLOW__CORE__EXECUTOR=SequentialExecutor
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_writer:airflow_writer_password@ETL_postgres:5432/airflow_meta
