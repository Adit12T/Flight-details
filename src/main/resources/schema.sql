-- Works on H2 and MySQL 8+.
CREATE TABLE IF NOT EXISTS flight (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    airline           VARCHAR(40)   NOT NULL,
    origin_city       VARCHAR(60)   NOT NULL,
    origin_state      VARCHAR(40)   NOT NULL,
    origin_code       CHAR(3)       NOT NULL,
    destination_city  VARCHAR(60)   NOT NULL,
    destination_state VARCHAR(40)   NOT NULL,
    destination_code  CHAR(3)       NOT NULL,
    departure_date    DATE          NOT NULL,
    price             DECIMAL(10,2) NOT NULL CHECK (price > 0),
    seats_available   INT           NOT NULL CHECK (seats_available >= 0),
    CONSTRAINT uq_flight UNIQUE (airline, origin_code, destination_code, departure_date)
);
