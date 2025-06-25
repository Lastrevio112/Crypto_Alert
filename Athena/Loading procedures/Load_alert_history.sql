--This procedure appends to alert_history every 10 minutes, triggered by a lambda function, to give out real-time alerts

INSERT INTO crypto_star_schema.alert_history
(
    coin_desc, last_price, alert_ts, pct_24h, pct_12h, pct_4h, pct_1h, alert_level, dt
)
SELECT 
--ROW_NUMBER() OVER(ORDER BY av.alert_ts, av.coin_desc DESC) AS alert_id,
--CONCAT(CAST(av.coin_desc AS varchar(20)), '-', CAST(av.alert_ts AS varchar(100))) AS alert_id,
av.coin_desc, av.last_price, av.alert_ts, av.pct_24h, av.pct_12h, av.pct_4h, av.pct_1h, av.alert_level,
date(av.alert_ts) AS dt
FROM   crypto_star_schema.alert_view av
WHERE  alert_ts > (
       SELECT COALESCE(max(alert_ts), timestamp '1970-01-01') --this condition is for delta insert so it avoids duplicates
       FROM   crypto_star_schema.alert_history
);
