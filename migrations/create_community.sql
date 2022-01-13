DROP TABLE IF EXISTS community;
CREATE TABLE community (
id SERIAL,
name VARCHAR(50) UNIQUE NOT NULL,
access_key TEXT
);