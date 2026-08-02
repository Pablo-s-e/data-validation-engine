CREATE TABLE file_configuration (
    id_type_file VARCHAR(5) NOT NULL,
    type_file_name VARCHAR(255) NOT NULL,
    file_format VARCHAR(50) NOT NULL,
    source VARCHAR(50) NOT NULL,
    origin_path VARCHAR(255),
    periodicity VARCHAR(50),
    separator VARCHAR(50),
    header BOOLEAN,
    reprocessable BOOLEAN,
    user_id VARCHAR(50) NOT NULL,
    CONSTRAINT pk_file_configuration PRIMARY KEY (id_type_file)
);

INSERT INTO file_configuration (
    id_type_file,
    type_file_name,
    file_format,
    source,
    origin_path,
    periodicity,
    separator,
    header,
    reprocessable,
    user_id
) VALUES
('00001', 'tabla_madre_frc_multi_v5', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00002', 'tabla_madre_frc_v6_2', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00003', 'tabla_madre_lip_v8', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00004', 'tabla_madre_sot', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', false, false, 'pablo.serrano'),
('00005', 'tabla_madre_sot_multi', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', false, false, 'pablo.serrano'),
('00006', 'tabla_madre_v23', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00007', 'tabla_validacion', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00008', 'tabla_validacion_7m_5', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00009', 'tabla_validacion_5m', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00010', 'tabla_validacion_2m_5', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00011', 'tabla_validacion_1m', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00012', 'tabla_validacion_100k', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00013', 'tabla_validacion_int', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00014', 'tabla_validacion_decimal', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00015', 'tabla_validacion_date', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00016', 'tabla_validacion_integridad_ref', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00017', 'tabla_validacion_func', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano'),
('00018', 'tabla_validacion_test', 'csv', 'yiyit_bigdata', 'bu_tablas_madres_hist', 'puntual', ';', true, false, 'pablo.serrano');