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
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_writer:airflow_writer_password@ETL_postgres:5432/airflow_met

3. Запустите контейнеры:

```bash
docker-compose up --build
```

Дождитесь завершения инициализации:
Airflow может потребовать несколько минут для инициализации базы данных.

4. Откройте интерфейсы:

Airflow : http://localhost:8080
Mongo Express : http://localhost:8081
PgAdmin : http://localhost:6060



## Логика работы

### 1. Инициализация баз данных

#### MongoDB

- Создается база данных `ETL_MONGO_FINAL`.
- Создаются пользователи:
  - `${MONGO_INITDB_ROOT_USERNAME}`: администратор.
  - `${MONGO_READER_USERNAME}`: пользователь с правами только на чтение данных.

#### PostgreSQL

- Создаются две базы данных:
  - `etl_data`: основная база для хранения данных.
  - `airflow_meta`: используется Airflow для хранения метаданных.

- Создаются пользователи:
  - `${POSTGRES_USER}`: администратор базы данных.
  - `etl_reader`: пользователь с правами только на чтение данных.
  - `airflow_writer`: пользователь для записи метаданных Airflow.

---

### 2. Генерация данных в MongoDB

Airflow запускает DAG `Create_synth_data_for_mongo`, который создает синтетические данные в MongoDB.  
Данные генерируются для нескольких коллекций:

- `UserSessions`
- `ProductPriceHistory`
- `EventLogs`
- `SupportTickets`
- `UserRecommendations`
- `ModerationQueue`
- `SearchQueries`

---

### 3. Перенос данных в PostgreSQL

Airflow запускает DAG `etl`, который выполняет следующие шаги:

1. **Извлечение**:
   - Спарк подключается к MongoDB и извлекает данные.

2. **Преобразование**:
   - Удаляются пустые значения и дубликаты.
   - Преобразуются типы данных (например, массивы в JSONB).
   - Приводятся временные метки к формату PostgreSQL.

3. **Загрузка**:
   - Данные записываются в соответствующие таблицы PostgreSQL.

---

## Особые права доступа

#### MongoDB

- Администратор (`${MONGO_INITDB_ROOT_USERNAME}`): полный доступ ко всем базам данных.
- Читатель (`${MONGO_READER_USERNAME}`): имеет право только на чтение данных.

#### PostgreSQL

- `postgres_user`: администратор базы данных.
- `etl_reader`: имеет право только на чтение данных из таблиц `public` схемы.
- `airflow_writer`: имеет право на запись, обновление и выборку данных для метаданных Airflow.
