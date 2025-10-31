#!/usr/bin/env python3
"""
DP2 – Quote Assembler (Prefect)
What this does:
1) POST the scatter API to populate your SQS queue
2) Monitor queue counts for up to 3 minutes
3) Log SQS message counts
4) Receive messages in batches, parse MessageAttributes, delete each
5) Sort by order_no, assemble the phrase, and print a preview before sending
6) Send the final phrase to the submission queue with required attributes

Finished
"""

from __future__ import annotations

import json
from typing import Dict, List, Tuple

import boto3
import requests
from prefect import flow, task, get_run_logger

# constants 
UVAID = "xqd7aq"
SCATTER_API = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVAID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

REGION = "us-east-1"
EXPECTED_COUNT = 21
LONG_POLL_WAIT = 20
VISIBILITY_TIMEOUT = 60
RECEIVE_BATCH_SIZE = 10

# helper to create SQS client per task
def _sqs():
    """Create a fresh SQS client (do not pass across tasks)."""
    return boto3.client("sqs", region_name=REGION)

# tasks 
@task(retries=0)
def post_scatter() -> str:
    """POST the scatter API to populate your queue and return the queue URL (no retries to avoid re-populating)."""
    logger = get_run_logger()
    logger.info("POSTing to scatter API to populate the queue…")
    r = requests.post(SCATTER_API, timeout=30)
    r.raise_for_status()
    queue_url = r.json()["sqs_url"]
    logger.info(f"Queue populated → {queue_url}")
    return queue_url

# logging task
@task
def log_queue_counts(queue_url: str) -> None:
    """Log SQS counters once (available / not visible / delayed / total)."""
    logger = get_run_logger()
    s = _sqs()
    attrs = s.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    ).get("Attributes", {})
    a = int(attrs.get("ApproximateNumberOfMessages", 0))
    b = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    c = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
    total = a + b + c
    logger.info(f"Queue counts → available={a}, not_visible={b}, delayed={c}, total={total}")

# monitoring task
@task
def monitor_queue_counts(queue_url: str, max_secs: int = 180, interval: int = 15):
    """Monitor queue counts every `interval` seconds up to `max_secs`, then proceed."""
    logger = get_run_logger()
    import time as _t
    s = _sqs()
    elapsed = 0
    while elapsed < max_secs:
        attrs = s.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        ).get("Attributes", {})
        a = int(attrs.get("ApproximateNumberOfMessages", 0))
        b = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        c = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        logger.info(f"Monitor → available={a}, not_visible={b}, delayed={c}, total={a+b+c}")
        # If anything is available OR delayed dropped to 0 (most have surfaced), move on
        if a > 0 or c == 0:
            break
        _t.sleep(interval)
        elapsed += interval

# receiving/deleting task
@task
def receive_delete_all(queue_url: str) -> List[Tuple[int, str]]:
    """
    Receive messages in batches until 21 unique order_no are collected, parse (order_no, word),
    delete each message, and dedupe by order_no.
    Returns: list of (order_no:int, word:str) for unique messages.
    """
    logger = get_run_logger()
    s = _sqs()
    collected: List[Tuple[int, str]] = []
    seen_orders = set()
    logger.info("Starting long-poll receive loop…")

    # loop until all unique messages received
    while len(seen_orders) < EXPECTED_COUNT:
        resp = s.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=RECEIVE_BATCH_SIZE,
            WaitTimeSeconds=LONG_POLL_WAIT,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        # process batch
        msgs = resp.get("Messages", [])
        if not msgs:
            logger.info(f"No messages this poll. Unique={len(seen_orders)}/{EXPECTED_COUNT}. Continuing…")
            continue

        # process each message
        for m in msgs:
            attrs = m.get("MessageAttributes") or {}
            order_raw = (attrs.get("order_no") or {}).get("StringValue")
            word = (attrs.get("word") or {}).get("StringValue")

            # validate attributes
            if order_raw is None or word is None:
                logger.info("Skipping one message missing attributes.")
                s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                continue

            # parse order_no
            try:
                order_no = int(order_raw)
            except ValueError:
                logger.info(f"Skipping one message with non-integer order_no='{order_raw}'.")
                s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                continue

            # dedupe
            if order_no in seen_orders:
                logger.info(f"Duplicate order_no {order_no} detected. Deleting duplicate.")
                s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                continue
            
            # collect unique
            collected.append((order_no, word))
            seen_orders.add(order_no)
            s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

        logger.info(f"Batch processed — total_unique_collected={len(seen_orders)}")

    # Persist for grading audit
    with open(f"dp2_collected_{UVAID}.json", "w") as f:
        json.dump(collected, f, indent=2)

    logger.info("All unique messages received and deleted.")
    return collected

# assembly task
@task
def assemble_phrase(pairs: List[Tuple[int, str]]) -> str:
    """Sort by order_no, join words, print a preview and the final phrase."""
    logger = get_run_logger()
    pairs_sorted = sorted(pairs, key=lambda x: x[0])

    logger.info("--- PREVIEW (first/last 6 ordered pairs) ---")
    logger.info(f"HEAD: {pairs_sorted[:6]}")
    logger.info(f"TAIL: {pairs_sorted[-6:]}")

    # assemble final phrase
    words = [w for _, w in pairs_sorted]
    phrase = " ".join(words)
    phrase = (
        phrase.replace(" ,", ",")
              .replace(" .", ".")
              .replace(" !", "!")
              .replace(" ?", "?")
              .replace(" ;", ";")
              .replace(" :", ":")
    )

    logger.info("FINAL PHRASE:")
    logger.info(phrase)
    logger.info("-------------------------------------------")
    return phrase

# submission task
@task
def submit_solution(phrase: str, platform: str = "prefect") -> Dict:
    """Send the assembled phrase to the submission queue with required attributes (logs phrase before send)."""
    logger = get_run_logger()
    s = _sqs()
    body = f"DP2 submission from {UVAID} via {platform}"

    logger.info(f"[About to submit] Full phrase:\n{phrase}\n")

    # send message
    resp = s.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody=body,
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVAID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )

    # log response
    http_code = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    logger.info(f"Submit response HTTPStatusCode={http_code}, MessageId={resp.get('MessageId')}")
    return resp


# flow 
@flow(name="DP2 – Quote Assembler (Prefect)", log_prints=True)
def dp2_flow(do_post_scatter: bool = True):
    logger = get_run_logger()
    logger.info("Starting DP2 Prefect flow…")

    # 1) populate once
    queue_url = post_scatter() if do_post_scatter else requests.get(SCATTER_API, timeout=30).json()["sqs_url"]

    # 2) monitor before pulling (15s cadence, up to 3 min)
    monitor_queue_counts(queue_url)

    # 3) snapshot counts
    log_queue_counts(queue_url)

    # 4) collect, assemble, submit
    pairs = receive_delete_all(queue_url)
    phrase = assemble_phrase(pairs)
    resp = submit_solution(phrase, platform="prefect")

    logger.info("Flow complete.")
    logger.info(json.dumps(resp, indent=2, default=str))


if __name__ == "__main__":
    dp2_flow(do_post_scatter=True)