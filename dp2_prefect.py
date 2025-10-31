#!/usr/bin/env python3
"""
DP2 – Quote Assembler Prefect
What this does:
1) POST the scatter API to populate your SQS queue 
2) Log SQS message counts 
3) Receive messages in batches, parse MessageAttributes, delete each
4) Sort by order_no, assemble the phrase, and print a preview before sending
5) Send the final phrase to the submission queue with required attributes

Finalized
"""

from __future__ import annotations

import json
from typing import Dict, List, Tuple

import boto3
import requests
from prefect import flow, task, get_run_logger

#constants
UVAID = "xqd7aq"
SCATTER_API = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVAID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

REGION = "us-east-1"
EXPECTED_COUNT = 21            # the API always scatters 21 messages
LONG_POLL_WAIT = 20            # seconds per receive call
VISIBILITY_TIMEOUT = 60        # seconds
RECEIVE_BATCH_SIZE = 10        # SQS max per request


def _sqs():
    """Create a fresh SQS client (do not pass across tasks)."""
    return boto3.client("sqs", region_name=REGION)


#tasks
@task
def post_scatter() -> str:
    #post the scatter API to populate your queue and return the queue URL
    logger = get_run_logger()
    logger.info("POSTing to scatter API to populate the queue...")
    r = requests.post(SCATTER_API, timeout=30)
    r.raise_for_status()
    queue_url = r.json()["sqs_url"]
    logger.info(f"Queue populated → {queue_url}")
    return queue_url


@task
def log_queue_counts(queue_url: str) -> None:
    #Log SQS counters once (available / not visible / delayed / total)
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
    print(f"Queue counts → available={a}, not_visible={b}, delayed={c}, total={total}")


@task
def receive_delete_all(queue_url: str) -> List[Tuple[int, str]]:
    """
    Receive messages in batches until all 21 are collected, parse (order_no, word),
    and delete each message using its ReceiptHandle.
    Returns: list of (order_no:int, word:str)
    """
    logger = get_run_logger()
    s = _sqs()
    collected: List[Tuple[int, str]] = []
    seen_orders = set()  # Track unique order_no values to handle duplicates
    logger.info("Starting long-poll receive loop...")

    while len(seen_orders) < EXPECTED_COUNT:
        
        # Long-poll for messages
        resp = s.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=RECEIVE_BATCH_SIZE,
            WaitTimeSeconds=LONG_POLL_WAIT,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        # Check for messages in this batch
        msgs = resp.get("Messages", [])
        if not msgs:
            logger.info(f"No messages this poll. Collected {len(seen_orders)}/{EXPECTED_COUNT}. Continuing...")
            continue
        
        # Process each message in the batch
        for m in msgs:
            attrs = m.get("MessageAttributes") or {}
            order_raw = (attrs.get("order_no") or {}).get("StringValue")
            word = (attrs.get("word") or {}).get("StringValue")
            if order_raw is None or word is None:
                logger.info("Skipping one message missing attributes.")
                continue

            try:
                order_no = int(order_raw)
            except ValueError:
                logger.info(f"Skipping one message with non-integer order_no='{order_raw}'.")
                continue

            # Skip duplicates 
            if order_no in seen_orders:
                logger.info(f"Duplicate order_no {order_no} detected. Skipping duplicate message.")
                s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                continue

            collected.append((order_no, word))
            seen_orders.add(order_no)
            s.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

        logger.info(f"Received+deleted batch={len(msgs)} | total_unique_collected={len(seen_orders)}")
    # End of receive loop
    logger.info("All unique messages received and deleted.")
    return collected


@task
def assemble_phrase(pairs: List[Tuple[int, str]]) -> str:
    """Sort by order_no, join words, print a preview and the final phrase."""
    pairs_sorted = sorted(pairs, key=lambda x: x[0])

    # quick preview to verify order
    print("\n--- PREVIEW (first/last 6 ordered pairs) ---")
    print("HEAD:", pairs_sorted[:6])
    print("TAIL:", pairs_sorted[-6:])

    # assemble final phrase with punctuation fixes
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

    # final output for testing
    print("\nFINAL PHRASE:\n")
    print(phrase)
    print("\n-------------------------------------------\n")
    return phrase


@task
def submit_solution(phrase: str, platform: str = "prefect") -> Dict:
    """Send the assembled phrase to the submission queue with required attributes."""
    s = _sqs()
    body = f"DP2 submission from {UVAID} via {platform}"
    
    # Send message
    resp = s.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody=body,
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVAID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform},
        },
    )
    http_code = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    print(f"Submit response HTTPStatusCode={http_code}, MessageId={resp.get('MessageId')}")
    return resp


# flow 
@flow(name="DP2 – Quote Assembler (Prefect)", log_prints=True)
def dp2_flow(do_post_scatter: bool = True):
    """
    End-to-end run:
      - POST scatter API (or skip if re-running on a schedule)
      - Log queue counts once
      - Receive/parse/delete all messages
      - Assemble & preview phrase
      - Submit to grader queue
    """
    print("Starting DP2 Prefect flow…")

    queue_url = post_scatter() if do_post_scatter else requests.get(SCATTER_API, timeout=30).json()["sqs_url"]
    log_queue_counts(queue_url)

    pairs = receive_delete_all(queue_url)
    phrase = assemble_phrase(pairs)
    resp = submit_solution(phrase, platform="prefect")

    print("\nFlow complete.")
    print(json.dumps(resp, indent=2, default=str))

# initiate flow
if __name__ == "__main__":
    dp2_flow(do_post_scatter=True)