CREATE TABLE trigger_control (
    id_trigger INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_type_file VARCHAR(5) NOT NULL,
    file_name VARCHAR NOT NULL,
    environment VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    date_load VARCHAR NOT NULL,
    tst_trigger_control TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    flag INTEGER,
    timestamp_load BIGINT,
    row_count VARCHAR,
    CONSTRAINT fk_trigger_id_type_file FOREIGN KEY (id_type_file) REFERENCES file_configuration(id_type_file)
);