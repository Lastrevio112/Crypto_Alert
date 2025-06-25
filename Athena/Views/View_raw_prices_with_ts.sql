CREATE OR REPLACE VIEW crypto_project.raw_prices_with_ts 
AS 
SELECT 
name , 
priceUSD , 
regexp_extract(
    "$path", 
    '.*prices_([0-9]{8}T[0-9]{6}Z)\.csv$', 
    1) AS ingest_ts 
FROM crypto_project.raw_prices;
