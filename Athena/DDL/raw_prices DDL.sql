CREATE EXTERNAL TABLE IF NOT EXISTS crypto_project.raw_prices (
  name       string,
  priceUSD   double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://crypto-project-csv-dump/'
TBLPROPERTIES ('skip.header.line.count'='1');
