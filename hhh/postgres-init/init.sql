\c postgres

DROP DATABASE IF EXISTS mydb;

CREATE DATABASE mydb;

\c mydb

CREATE TABLE IF NOT EXISTS file_metadata (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    path_file_s3 VARCHAR(500),
    upload_timestamp TIMESTAMP,
    status VARCHAR(50) DEFAULT 'uploaded'
);
