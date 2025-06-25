CREATE EXTERNAL TABLE IF NOT EXISTS crypto_star_schema.d_coin (
  coin_id    int,
  coin_desc  string
)
STORED AS PARQUET
LOCATION 's3://crypto-project-star/d_coin/';
