--Gets alerts in text format from the last three days
CREATE OR REPLACE VIEW crypto_star_schema.text_alerts AS
WITH ranked_alerts AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY coin_desc ORDER BY alert_ts DESC) AS rn
    FROM crypto_star_schema.alert_history
    WHERE DATE_DIFF('day', DATE(alert_ts), CURRENT_DATE) <= 3
)
SELECT 
    CASE 
        WHEN alert_level = 'PCT10_24H' THEN (coin_desc || ' has changed its price by ' || CAST(pct_24h AS varchar(5)) || '% in the last 24 hours, as of ' || CAST(alert_ts AS varchar(25)))
        WHEN alert_level = 'PCT7_12H' THEN (coin_desc || ' has changed its price by ' || CAST(pct_12h AS varchar(5)) || '% in the last 12 hours, as of ' || CAST(alert_ts AS varchar(25)))
        WHEN alert_level = 'PCT5_4H' THEN (coin_desc || ' has changed its price by ' || CAST(pct_4h AS varchar(5)) || '% in the last 4 hours, as of ' || CAST(alert_ts AS varchar(25)))
        WHEN alert_level = 'PCT3_1H' THEN (coin_desc || ' has changed its price by ' || CAST(pct_1h AS varchar(5)) || '% in the last hour, as of ' || CAST(alert_ts AS varchar(25)))
    END AS alert_string
FROM ranked_alerts
WHERE rn = 1;
