CREATE DATABASE etl_data;

\connect etl_data;

CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'secret';

CREATE USER etl_reader WITH PASSWORD 'etl_reader_password';
GRANT CONNECT ON DATABASE etl_data TO etl_reader;
GRANT USAGE ON SCHEMA public TO etl_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO etl_reader;

CREATE TABLE IF NOT EXISTS UserSessions (
    session_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    pages_visited JSONB,          -- Массив URL в формате JSONB
    device TEXT,                  -- Тип устройства
    actions JSONB                 -- Массив действий в формате JSONB
);

CREATE TABLE IF NOT EXISTS ProductPriceHistory (
    product_id INT PRIMARY KEY,
    price_changes JSONB,           -- Массив объектов в формате JSONB
    current_price DECIMAL(10, 2),  -- Текущая цена продукта
    currency VARCHAR(10)           -- Валюта
);

CREATE TABLE IF NOT EXISTS EventLogs (
    event_id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL, -- Время события
    event_type VARCHAR(255),        -- Тип события
    details TEXT                    -- Детали события
);

CREATE TABLE IF NOT EXISTS SupportTickets (
    ticket_id UUID PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50),             -- Статус тикета
    issue_type VARCHAR(255),        -- Тип проблемы
    messages JSONB,                 -- Массив сообщений в формате JSONB
    created_at TIMESTAMPTZ,         -- Время создания
    updated_at TIMESTAMPTZ          -- Время обновления
);

CREATE TABLE IF NOT EXISTS UserRecommendations (
    user_id INT PRIMARY KEY,
    recommended_products JSONB,     -- Массив рекомендованных продуктов в формате JSONB
    last_updated TIMESTAMPTZ        -- Время последнего обновления
);

CREATE TABLE IF NOT EXISTS ModerationQueue (
    review_id UUID PRIMARY KEY,
    user_id INT,
    product_id INT,
    review_text TEXT,               -- Текст отзыва
    rating INT,                     -- Рейтинг отзыва
    moderation_status VARCHAR(50),  -- Статус модерации
    flags JSONB,                    -- Массив флагов в формате JSONB
    submitted_at TIMESTAMPTZ        -- Время подачи отзыва
);

CREATE TABLE IF NOT EXISTS SearchQueries (
    query_id UUID PRIMARY KEY,
    user_id INT,
    query_text TEXT,                -- Текст поискового запроса
    timestamp TIMESTAMPTZ NOT NULL, -- Время запроса
    filters JSONB,                  -- Массив фильтров в формате JSONB
    results_count INT               -- Количество результатов
);

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
    dttm TIMESTAMPTZ NOT NULL,     -- Время события
    event VARCHAR(255) NOT NULL,    -- Событие
    owner VARCHAR(255) NOT NULL,    -- Владелец
    extra JSON NOT NULL             -- Дополнительная информация (например, контекст ошибки)
);