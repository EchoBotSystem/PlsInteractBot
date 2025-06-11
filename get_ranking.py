import json
import time
from collections import Counter
import os
import boto3
import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class RankingUser:
    user_id: str
    message_count: int


def get_ranking(
    start_unixtime: int = int(time.time() * 1000)
    - (60 * 60 * 24 * 30 * 1000),  # Now minus 30 days in milliseconds
    end_unixtime: int = int(time.time() * 1000),  # Now in milliseconds
) -> list[
    dict[
        str,  # user_id
        int,  # message_count
    ]
]:
    print(f"Getting ranking messages from Unix time {start_unixtime} to {end_unixtime}")

    dynamodb = boto3.client("dynamodb")
    all_messages = []
    last_evaluated_key = None

    while True:
        scan_params = {
            "TableName": "comments",
            "ProjectionExpression": "chatter_user_id, reception_unixtime",
            "FilterExpression": "reception_unixtime BETWEEN :start_time AND :end_time",
            "ExpressionAttributeValues": {
                ":start_time": {"N": str(start_unixtime)},
                ":end_time": {"N": str(end_unixtime)},
            },
        }
        if last_evaluated_key:
            scan_params["ExclusiveStartKey"] = last_evaluated_key
        response = dynamodb.scan(**scan_params)
        all_messages.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    chatter_message_counts = Counter()
    for item in all_messages:
        chatter_user_id = item.get("chatter_user_id", {}).get("S")
        if chatter_user_id:
            chatter_message_counts[chatter_user_id] += 1

    print(f"Aggregated chatter message counts: {chatter_message_counts}")

    top_chatters = chatter_message_counts.most_common(10)
    return [
        {"user_id": user_id, "message_count": count} for user_id, count in top_chatters
    ]

