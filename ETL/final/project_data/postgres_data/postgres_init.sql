CREATE DATABASE etl_data;

\connect etl_data;

CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'secret';

CREATE USER etl_reader WITH PASSWORD 'etl_reader_password';
GRANT CONNECT ON DATABASE etl_data TO etl_reader;
GRANT USAGE ON SCHEMA public TO etl_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO etl_reader;

CREATE DATABASE airflow_meta;

\connect airflow_meta;

CREATE USER airflow_writer WITH PASSWORD 'airflow_writer_password';
GRANT CONNECT ON DATABASE airflow_meta TO airflow_writer;
GRANT CREATE ON SCHEMA public TO airflow_writer;
GRANT USAGE, SELECT ON SEQUENCE log_id_seq TO airflow_writer;
GRANT USAGE ON SCHEMA public TO airflow_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, SELECT ON TABLES TO airflow_writer;

CREATE TABLE IF NOT EXISTS log (
    id SERIAL PRIMARY KEY,
    dttm TIMESTAMPTZ NOT NULL,
    event VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    extra JSON NOT NULL
);

--CREATE TABLE IF NOT EXISTS UserSessions (
--);
--
--CREATE TABLE IF NOT EXISTS ProductPriceHistory (
--);
--
--CREATE TABLE IF NOT EXISTS EventLogs (
--);
--
--CREATE TABLE IF NOT EXISTS SupportTickets (
--);
--
--CREATE TABLE IF NOT EXISTS UserRecommendations (
--);
--
--CREATE TABLE IF NOT EXISTS ModerationQueue (
--);
--
--CREATE TABLE IF NOT EXISTS SearchQueries (
--);


