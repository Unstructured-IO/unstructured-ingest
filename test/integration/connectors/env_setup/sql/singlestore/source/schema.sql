CREATE DATABASE ingest_test;
USE ingest_test;

CREATE TABLE cars (
    car_id SERIAL PRIMARY KEY,
    brand TEXT NOT NULL,
    price INTEGER NOT NULL
);
