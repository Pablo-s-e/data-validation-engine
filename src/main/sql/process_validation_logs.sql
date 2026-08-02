CREATE TABLE process_validation_logs (
    id_trigger INTEGER NOT NULL,
    validation_id VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    type_validation VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
    validation_msg VARCHAR,
    field_name VARCHAR,
    incidences VARCHAR NOT NULL,
    flag INTEGER NOT NULL,
    execution_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    date_load VARCHAR NOT NULL,
    timestamp_load BIGINT NOT NULL,
    CONSTRAINT fk_process_validation_id_trigger FOREIGN KEY (id_trigger) REFERENCES trigger_control(id_trigger)
);