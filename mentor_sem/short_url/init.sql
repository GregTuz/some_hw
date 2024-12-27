CREATE TABLE IF NOT EXISTS short_url (
    full_name VARCHAR(255) NOT NULL PRIMARY KEY,
    short_url TEXT NOT NULL
);

INSERT INTO short_url (full_name, short_url) VALUES
('http://localhost:6060/browser/', 'http://localhost/PgAdmin');
