CREATE OR REPLACE VIEW crypto_star_schema.f_price AS 
SELECT
  row_number() OVER (ORDER BY name) AS row_id,
  c.coin_id,
  r.priceUSD AS price,
  'USD' AS currency,
  date(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''')) AS date,
  hour(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''')) AS hour,
  minute(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''')) AS minute,
  CAST(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''') AS timestamp) AS ts
FROM crypto_project.raw_prices_with_ts r
  JOIN crypto_star_schema.d_coin c ON r.name = c.coin_desc
  JOIN crypto_star_schema.d_time dt 
    ON date(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''')) = dt.date
WHERE DATE_DIFF('day', date(parse_datetime(r.ingest_ts,'yyyyMMdd''T''HHmmss''Z''')), current_date) <= 7;
