# -*- coding: utf-8 -*-
"""
Lambda: load_alert_history_lambda
--------------------------------
Executes a **saved Athena query (NamedQuery)** that already contains the
`INSERT INTO crypto_star_schema.alert_history …` statement.  Supplying the
NamedQuery ID keeps SQL out of the function and lets analysts edit the query
in the Athena console without redeploying code.

Required IAM permissions for the Lambda execution role
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* athena:StartQueryExecution
* athena:GetQueryExecution
* athena:GetNamedQuery
* s3:GetObject, s3:PutObject, s3:ListBucket – on the Athena *query‑results* bucket
* s3:PutObject – on the *alert_history data* bucket

Environment variables
~~~~~~~~~~~~~~~~~~~~~
ATHENA_DATABASE   – Glue database (e.g. ``crypto_star_schema``)
NAMED_QUERY_ID    – ID of the saved query holding the INSERT
OUTPUT_LOCATION   – ``s3://bucket/prefix/`` for Athena results
WORKGROUP         – optional; defaults to ``primary`` if omitted
"""

import os
import time
import logging
from typing import Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ATHENA = boto3.client("athena")

# ---------------------------------------------------------------------------
# Polling configuration
# ---------------------------------------------------------------------------
_POLL_INTERVAL_SEC = 2  # time between GetQueryExecution calls
_MAX_WAIT_SEC = 900      # bail out after 15 minutes (shouldn’t happen)


def lambda_handler(event: Dict, context):  # noqa: D401  (AWS entry point)
    """Run the NamedQuery that appends new alerts to *alert_history*."""

    database = os.environ["ATHENA_DATABASE"]
    named_query_id = os.environ["NAMED_QUERY_ID"]
    output = os.environ["OUTPUT_LOCATION"]
    workgroup = os.getenv("WORKGROUP", "primary")

    # -------------------------------------------------------------------
    # Retrieve the SQL text from the saved query
    # -------------------------------------------------------------------
    try:
        query_def = ATHENA.get_named_query(NamedQueryId=named_query_id)
        query_string = query_def["NamedQuery"]["QueryString"]
    except ClientError:
        logger.exception("Could not fetch NamedQuery %s", named_query_id)
        raise

    logger.info("Executing NamedQuery %s in DB %s", named_query_id, database)

    # -------------------------------------------------------------------
    # Kick off execution
    # -------------------------------------------------------------------
    try:
        start = ATHENA.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output},
            WorkGroup=workgroup,
        )
    except ClientError:
        logger.exception("StartQueryExecution failed")
        raise

    qid = start["QueryExecutionId"]
    logger.info("QueryExecutionId: %s", qid)

    # -------------------------------------------------------------------
    # Poll until Athena finishes (or fails)
    # -------------------------------------------------------------------
    elapsed = 0
    while True:
        exec_resp = ATHENA.get_query_execution(QueryExecutionId=qid)
        state = exec_resp["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        time.sleep(_POLL_INTERVAL_SEC)
        elapsed += _POLL_INTERVAL_SEC
        if elapsed >= _MAX_WAIT_SEC:
            raise TimeoutError(
                f"Query {qid} did not finish within {_MAX_WAIT_SEC}s; last state={state}"
            )

    if state != "SUCCEEDED":
        reason = exec_resp["QueryExecution"]["Status"].get("StateChangeReason", "")
        logger.error("Athena query failed (%s): %s", state, reason)
        raise RuntimeError(f"Athena query {qid} failed: {state} – {reason}")

    stats = exec_resp["QueryExecution"]["Statistics"]
    logger.info(
        "Query succeeded in %.1f s – scanned %.2f MB",
        elapsed,
        stats.get("DataScannedInBytes", 0) / (1024 ** 2),
    )

    # Return a compact summary (ends up in CloudWatch Logs and the Invoke reply)
    return {
        "query_id": qid,
        "data_scanned_mb": round(stats.get("DataScannedInBytes", 0) / (1024 ** 2), 3),
        "execution_time_ms": stats.get("EngineExecutionTimeInMillis", 0),
    }
