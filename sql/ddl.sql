DROP TABLE IF EXISTS device_types;
CREATE TABLE device_types (
    id        INT PRIMARY KEY,
    type_name VARCHAR(100)
);

DROP TABLE IF EXISTS iot_aggregates;
CREATE TABLE iot_aggregates (
    event_time      VARCHAR(10),
    device_type     VARCHAR(100),
    avg_temp        DOUBLE PRECISION,
    median_humidity DOUBLE PRECISION,
    min_temp        DOUBLE PRECISION,
    max_temp        DOUBLE PRECISION,
    readings_count  INT,
    status          VARCHAR(50)
);