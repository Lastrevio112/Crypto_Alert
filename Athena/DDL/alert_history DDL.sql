CREATE EXTERNAL TABLE IF NOT EXISTS crypto_star_schema.alert_history (
    coin_desc   string,
    alert_ts    timestamp,
    last_price  double,
    pct_24h     double,
    pct_12h     double,
    pct_4h      double,
    pct_1h      double,
    alert_level varchar(9)
)
PARTITIONED BY (dt date)                -- declare dt here
STORED AS PARQUET                       -- file format
LOCATION 's3://crypto-project-alert-history/';
