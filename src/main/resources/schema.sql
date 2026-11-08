DROP TABLE IF EXISTS demo_job;

CREATE TABLE demo_job (
    id BIGINT PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    execute_count INT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0
)
