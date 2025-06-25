CREATE OR REPLACE VIEW crypto_star_schema.alert_view AS 
WITH latest AS (
    -- latest price for every coin
    SELECT  coin_id,
            MAX(ts)       AS last_ts,
            MAX_BY(price, ts) AS last_price
    FROM     crypto_star_schema.f_price
    GROUP BY coin_id
),
older AS (
    SELECT  l.coin_id,
            l.last_price,
            l.last_ts,
            /* price 24 h ago (same coin) */
            max_by(p.price, p.ts) FILTER (WHERE p.ts <= l.last_ts - INTERVAL '24' HOUR) 
                AS price_24h,
            /* 12 h and 6 h analogously */
            max_by(p.price, p.ts) FILTER (WHERE p.ts <= l.last_ts - INTERVAL '12' HOUR)
                AS price_12h,
            max_by(p.price, p.ts) FILTER (WHERE p.ts <= l.last_ts - INTERVAL '4'  HOUR)
                AS price_4h,
            max_by(p.price, p.ts) FILTER (WHERE p.ts <= l.last_ts - INTERVAL '1'  HOUR)  
                AS price_1h
    FROM latest l
    JOIN f_price p
      ON p.coin_id = l.coin_id
    GROUP BY l.coin_id, l.last_price, l.last_ts
)
SELECT  d.coin_desc,
        last_price,
        ROUND(100*(last_price/price_24h - 1),2) AS pct_24h,
        ROUND(100*(last_price/price_12h - 1),2) AS pct_12h,
        ROUND(100*(last_price/price_4h  - 1),2) AS pct_4h,
        ROUND(100*(last_price/price_1h - 1),2) AS pct_1h,
        CASE
            WHEN last_price/price_24h - 1 >= 0.10 THEN 'PCT10_24H'
            WHEN last_price/price_12h - 1 >= 0.07 THEN 'PCT7_12H'
            WHEN last_price/price_4h  - 1 >= 0.05 THEN 'PCT5_4H'
            WHEN last_price/price_1h  - 1 >= 0.03 THEN 'PCT3_1H'
        END                                    AS alert_level,
        last_ts                                 AS alert_ts
FROM    older
JOIN    d_coin d USING (coin_id)
WHERE   (CASE
            WHEN last_price/price_1h  - 1 >= 0.03 THEN 'PCT3_1H'
            WHEN last_price/price_4h  - 1 >= 0.05 THEN 'PCT5_4H'
            WHEN last_price/price_12h - 1 >= 0.07 THEN 'PCT7_12H'
            WHEN last_price/price_24h - 1 >= 0.10 THEN 'PCT10_24H'
        END)
IS NOT NULL
AND d.coin_desc NOT IN ('Aave', 'Wrapped Fantom'); --these two coins failed in the pipeline
