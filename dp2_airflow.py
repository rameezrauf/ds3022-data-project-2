# -*- coding: utf-8 -*-
"""
Airflow DAG 
- Uses @dag / @task decorators
- Rubric-compliant logging via LoggingMixin
- Monitors queue, deduplicates duplicate messages, previews phrase, logs full phrase before submit
finished
"""

import time
from datetime import datetime, timedelta
from typing import List, Tuple

import boto3
import requests
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin

# configuration
UVA_ID = "xqd7aq"
AWS_REGION = "us-east-1"
TARGET_MESSAGE_COUNT = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
SCATTER_API_TMPL = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"

# rubric logger
log = LoggingMixin().log

# flow
@dag(
    dag_id="dp2_airflow",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ds3022", "project", "sqs"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)

# flow function
def dp2_airflow_dag():
    """Airflow DAG to solve the SQS puzzle:
    1) Populate the queue
    2) Monitor queue briefly
    3) Log counts once
    4) Collect all unique messages
    5) Reassemble phrase
    6) Submit solution
    """
    
    # tasks
    @task(retries=0)
    def populate_queue(uva_id: str) -> str:
        """POST to scatter to populate the SQS queue (no retries to avoid re-populating)."""
        url = SCATTER_API_TMPL.format(uva_id=uva_id)
        log.info(f"Task 1: Populating queue for '{uva_id}' at {url}…")
        resp = requests.post(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        sqs_url = payload.get("sqs_url")
        if not sqs_url:
            raise ValueError("API response missing 'sqs_url'.")
        log.info(f"Task 1: Success! Queue URL: {sqs_url}")
        return sqs_url

    # monitor task
    @task
    def monitor_queue(queue_url: str, max_secs: int = 180, interval: int = 15) -> None:
        """Monitor queue counts every `interval` seconds up to `max_secs`."""
        s = boto3.client("sqs", region_name=AWS_REGION)
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
            log.info(f"Monitor → available={a}, not_visible={b}, delayed={c}, total={a+b+c}")
            if a > 0 or c == 0:
                break
            time.sleep(interval)
            elapsed += interval

    # count task
    @task
    def count_messages(queue_url: str) -> None:
        """Log one snapshot of SQS counts."""
        s = boto3.client("sqs", region_name=AWS_REGION)
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
        log.info(f"Counts → available={a}, not_visible={b}, delayed={c}, total={a+b+c}")

    # collect task
    @task(retries=3, execution_timeout=timedelta(minutes=20))
    def collect_messages(queue_url: str) -> List[Tuple[int, str]]:
        """Receive, parse, delete, and dedupe by order_no until 21 unique messages are collected."""
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        log.info(f"Task 2: Starting collector for queue: {queue_url}")
        log.info(f"Task 2: Target is {TARGET_MESSAGE_COUNT} unique messages.")

        collected: List[Tuple[int, str]] = []
        seen_orders = set()

        # collection loop
        while len(seen_orders) < TARGET_MESSAGE_COUNT:
            # receive messages
            try:
                log.info(f"Task 2: [{len(seen_orders)}/{TARGET_MESSAGE_COUNT}] Polling (20s)…")
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    MessageAttributeNames=["All"],
                )
                msgs = response.get("Messages", [])
                if not msgs:
                    log.info("Task 2: No new messages. Continuing…")
                    continue

                # process messages
                for msg in msgs:
                    attrs = msg.get("MessageAttributes", {})
                    order_raw = (attrs.get("order_no") or {}).get("StringValue")
                    word = (attrs.get("word") or {}).get("StringValue")

                    # validate attributes
                    if order_raw is None or word is None:
                        log.warning("Task 2: Skipping malformed message.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue
                    
                    # parse order_no
                    try:
                        order_no = int(order_raw)
                    except ValueError:
                        log.warning(f"Task 2: Invalid order_no '{order_raw}', skipping.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue

                    # dedupe
                    if order_no in seen_orders:
                        log.info(f"Task 2: Duplicate order_no {order_no}, deleting duplicate.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue

                    collected.append((order_no, word))
                    seen_orders.add(order_no)
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

                    # check completion
                    if len(seen_orders) >= TARGET_MESSAGE_COUNT:
                        log.info("Task 2: All messages collected!")
                        break

                log.info(f"Task 2: Batch processed — total_unique={len(seen_orders)}")

            # catch errors
            except Exception as e:
                log.error(f"Task 2: Error during collection loop: {e}", exc_info=True)
                time.sleep(10)

        log.info("Task 2: Collection complete.")
        return collected

    # assembly task
    @task
    def reassemble_phrase(data: List[Tuple[int, str]]) -> str:
        """Sorts collected data and joins into final phrase; logs assembled phrase."""
        if not data:
            raise ValueError("No messages collected; cannot assemble phrase.")
        log.info("Task 3: Reassembling phrase…")
        data.sort(key=lambda x: x[0])
        words = [word for _, word in data]
        phrase = " ".join(words)
        log.info(f"Task 3: Assembled phrase: {phrase}")
        return phrase

    # submission task
    @task(retries=3)
    def submit_solution(uva_id: str, phrase: str) -> str:
        """Submits the final phrase to the grader queue (logs full phrase before send)."""
        if not phrase:
            raise ValueError("Empty phrase; nothing to submit.")
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        log.info(f"Task 4: Submitting solution to {SUBMISSION_URL}…")
        log.info(f"Task 4: Phrase to be submitted:\n{phrase}")

        # send message
        try:
            response = sqs.send_message(
                QueueUrl=SUBMISSION_URL,
                MessageBody=f"Submission from {uva_id}",
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": uva_id},
                    "phrase": {"DataType": "String", "StringValue": phrase},
                    "platform": {"DataType": "String", "StringValue": "airflow"},
                },
            )
            message_id = response.get("MessageId", "N/A")
            log.info(f"Task 4: Submit successful — MessageId={message_id}")
            return message_id
        
        # catch errors
        except Exception as e:
            log.error(f"Task 4: Failed to submit solution: {e}", exc_info=True)
            raise

    # flow wiring
    sqs_url = populate_queue(uva_id=UVA_ID)
    monitor_queue(sqs_url)
    count_messages(sqs_url)
    collected = collect_messages(sqs_url)
    phrase = reassemble_phrase(collected)
    _ = submit_solution(uva_id=UVA_ID, phrase=phrase)

# Instantiate the DAG
dp2_airflow_dag()