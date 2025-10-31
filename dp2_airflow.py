# -*- coding: utf-8 -*-
"""
Airflow DAG — SQS Puzzle
- Uses @dag / @task decorators
- Rubric-compliant logging via LoggingMixin
- Deduplicates duplicate messages

Finished
"""

import time
from datetime import datetime, timedelta
from typing import List, Tuple

import boto3
import requests
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin

# Configuration
UVA_ID = "xqd7aq"
AWS_REGION = "us-east-1"
TARGET_MESSAGE_COUNT = 21
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

# logger
log = LoggingMixin().log


@dag(
    dag_id="dp2_airflow",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ds3022", "project", "sqs"]
)

# DAG Definition
def dp2_airflow_dag():
    """
    Airflow DAG to solve the SQS puzzle:
    1. Populate the queue.
    2. Collect all messages.
    3. Reassemble the phrase.
    4. Submit the solution.
    """

    # Task 1: Populate Queue
    @task(retries=3)
    def populate_queue(uva_id: str) -> str:
        """Sends a POST request to the API to populate the SQS queue."""
        url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
        log.info(f"Task 1: Populating queue for '{uva_id}' at {url}...")

        # Make the API request
        try:
            response = requests.post(url, timeout=30)
            response.raise_for_status()
            payload = response.json()

            if "sqs_url" not in payload:
                raise ValueError("API Error: 'sqs_url' not found in response.")

            sqs_url = payload["sqs_url"]
            log.info(f"Task 1: Success! Queue URL: {sqs_url}")
            return sqs_url

        # Handle request exceptions
        except requests.RequestException as e:
            log.error(f"Task 1: API request failed: {e}")
            raise

    # Task 2: Collect Messages
    # collect messages from SQS, deduplicate, and return list of (order_no, word)
    @task(retries=3, execution_timeout=timedelta(minutes=20))
    def collect_messages(queue_url: str) -> List[Tuple[int, str]]:
        """Receives messages in batches, parses, deletes, and deduplicates them."""
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        log.info(f"Task 2: Starting collector for queue: {queue_url}")
        log.info(f"Task 2: Target is {TARGET_MESSAGE_COUNT} unique messages.")

        collected: List[Tuple[int, str]] = []
        seen_orders = set()

        # Collection loop
        while len(seen_orders) < TARGET_MESSAGE_COUNT:
            try:
                log.info(f"Task 2: [{len(seen_orders)}/{TARGET_MESSAGE_COUNT}] Polling for messages (20s)...")
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    MessageAttributeNames=["All"]
                )
                
                # Check for messages
                msgs = response.get("Messages", [])
                if not msgs:
                    log.info(f"Task 2: No new messages. Continuing...")
                    continue

                # Process received messages
                for msg in msgs:
                    attrs = msg.get("MessageAttributes", {})
                    order_raw = (attrs.get("order_no") or {}).get("StringValue")
                    word = (attrs.get("word") or {}).get("StringValue")

                    # Validate message
                    if order_raw is None or word is None:
                        log.warning("Task 2: Skipping malformed message.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue

                    # Parse order number
                    try:
                        order_no = int(order_raw)
                    except ValueError:
                        log.warning(f"Task 2: Invalid order_no '{order_raw}', skipping.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue
                    
                    # Deduplicate
                    if order_no in seen_orders:
                        log.info(f"Task 2: Duplicate order_no {order_no}, deleting duplicate.")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                        continue
                    
                    # New unique message
                    collected.append((order_no, word))
                    seen_orders.add(order_no)
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

                    # Check if target reached
                    if len(seen_orders) >= TARGET_MESSAGE_COUNT:
                        log.info("Task 2: All messages collected!")
                        break

                log.info(f"Task 2: Batch processed — total unique={len(seen_orders)}")
            
            # Handle unexpected exceptions
            except Exception as e:
                log.error(f"Task 2: Error during collection loop: {e}", exc_info=True)
                time.sleep(10)

        # Final log and return
        log.info("Task 2: Collection complete.")
        return collected

    # Task 3: Reassemble Phrase
    @task
    def reassemble_phrase(data: List[Tuple[int, str]]) -> str:
        """Sorts collected data and joins into final phrase."""
        if not data:
            raise ValueError("No messages collected; cannot assemble phrase.")

        # Reassemble
        log.info("Task 3: Reassembling phrase...")
        data.sort(key=lambda x: x[0])
        words = [word for _, word in data]
        phrase = " ".join(words)
        log.info(f"Task 3: Assembled phrase: {phrase}")
        return phrase

    # Task 4: Submit Solution
    @task(retries=3)
    def submit_solution(uva_id: str, phrase: str) -> str:
        """Submits the final phrase to the grader queue."""
        if not phrase:
            raise ValueError("Empty phrase; nothing to submit.")

        sqs = boto3.client("sqs", region_name=AWS_REGION)
        log.info(f"Task 4: Submitting solution to {SUBMISSION_URL}...")

        #log the full phrase before sending for testing
        log.info(f"Task 4: Phrase to be submitted:\n{phrase}")

        # Send message
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

        # Handle exceptions
        except Exception as e:
            log.error(f"Task 4: Failed to submit solution: {e}", exc_info=True)
            raise

    # Task Flow
    sqs_url = populate_queue(uva_id=UVA_ID)
    collected = collect_messages(sqs_url)
    phrase = reassemble_phrase(collected)
    _ = submit_solution(uva_id=UVA_ID, phrase=phrase)


# Instantiate DAG
dp2_airflow_dag()