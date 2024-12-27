CREATE TABLE IF NOT EXISTS todo_data (
    title VARCHAR(255) NOT NULL PRIMARY KEY,
    description TEXT,
    completed BOOLEAN DEFAULT FALSE
);

INSERT INTO todo_data (title, description) VALUES
('TEST-1', 'This is the first task');
