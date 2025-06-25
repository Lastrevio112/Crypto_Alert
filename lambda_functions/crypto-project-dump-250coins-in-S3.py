import os
import boto3
import csv
from io import StringIO
from datetime import datetime, timezone
from coinpaprika import client as Coinpaprika
import json

_s3 = boto3.client('s3')
BUCKET = os.environ['BUCKET_NAME']
PREFIX = os.environ.get('PREFIX', '').rstrip('/')
LAMBDA_PAUSED = os.environ['LAMBDA_PAUSED']

def fetch_top_n_prices(n: int = 250, quote: str = "USD"):
    api = Coinpaprika.Client()
    tickers = api.tickers(limit=n, quotes=quote)
    # Extract only (name, price)
    # Extract (name, price) via dict lookup
    rows = []
    for t in tickers:
        name = t['name']
        price = t['quotes'][quote]['price']
        rows.append((name, price))
    return rows

def lambda_handler(event, context):
    if LAMBDA_PAUSED == 'true':
        print("Exiting early because lambda is paused.")
        return

    # 1. Fetch data
    rows = fetch_top_n_prices(250, "USD")

    # 2. Build key with UTC timestamp
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"prices_{ts}.csv"
    key = f"{PREFIX}/{filename}" if PREFIX else filename

    # 3. Write CSV in memory
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(['name', 'priceUSD'])
    writer.writerows(rows)

    # 4. Upload to S3
    _s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=csv_buffer.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )

    # 5. Publish to EventBridge to trigger the Load_F_Price procedure
    eb = boto3.client("events")
    eb.put_events(
        Entries=[{
            "Source": "crypto-project",  # any namespace you choose
            "DetailType": "RawPricesCSVReady",
            "Detail": json.dumps({"ingest_ts": ts}),
            "EventBusName": "default"
        }]
    )

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(rows)} records to s3://{BUCKET}/{key}"
    }

#print(fetch_top_n_prices())
