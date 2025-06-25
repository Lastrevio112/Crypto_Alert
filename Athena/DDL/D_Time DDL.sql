-- 1. Drop if you need to re-create
DROP TABLE IF EXISTS crypto_star_schema.d_time;

-- 2. Create the table with every single date between 2020-01-01 and 2060-12-31
CREATE TABLE crypto_star_schema.d_time
WITH (
  format            = 'PARQUET',
  external_location = 's3://crypto-project-star/d_time/'
) AS
SELECT
  day_dt                          AS date,                      -- primary key = the date itself
  year(day_dt)                    AS year,
  month(day_dt)                   AS month_no,
  day(day_dt)                     AS day,
  date_format(day_dt, '%M')       AS month_text
FROM (
  SELECT 
    date_add('day', seq, DATE '2020-01-01') AS day_dt
  FROM 
    -- generate a sequence of integers from 0 up to the day-difference
    UNNEST(
      sequence(
        0, 
        date_diff('day', DATE '2020-01-01', DATE '2060-12-31')
      )
    ) AS t(seq)
) x
ORDER BY day_dt
;
