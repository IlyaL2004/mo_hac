CREATE DATABASE IF NOT EXISTS events;

CREATE TABLE company_indicators
(
    data_type       LowCardinality(String),          -- financial, company_info, contacts и т.д.
    period          Nullable(String),                -- '2023-05-15' или NULL
    inn             String,                          -- ИНН как строка (может начинаться с 0, и длина фиксирована)
    indicator_name  LowCardinality(String),          -- revenue, employee_count и т.д.
    indicator_value String,                          -- значение всегда как строка (универсально)
    source          LowCardinality(String),          -- spark, manual, egrul, tax_service
    update_date     Date                             -- дата обновления (для партиционирования)
)
ENGINE = MergeTree
ORDER BY (inn);



