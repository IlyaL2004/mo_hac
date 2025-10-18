CREATE DATABASE IF NOT EXISTS events;


CREATE TABLE events.company_indicators_typed (
    period Nullable(Int16),
    inn String,
    indicator_name String,
    string_value Nullable(String),
    numeric_value Nullable(Float64),
    bool_value Nullable(Bool),
    date_value Nullable(Date),

) ENGINE = MergeTree()
ORDER BY (inn);


