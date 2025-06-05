import boto3
import time

from collections import Counter
from boto3 import Session

def lambda_handler(event: dict, context: dict) -> dict:

    print("Rankings processor started.")

    dynamodb: Session = boto3.client("dynamodb")
    comments_table_name = "comments"
    rankings_table_name = "rankings"

    end_unixtime = int(time.time() * 1000) if event.get("end_unixtime") is None else int(event["end_unixtime"])
    start_unixtime = end_unixtime - (60*60*24*30 * 1000)

    print(f"Processing messages from Unix time {start_unixtime} to {end_unixtime}")

    all_messages = []
    last_evaluated_key = None

    while True:
        scan_params = {
            "TableName": comments_table_name,
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

    print(f"Found {len(all_messages)} messages in the last month seconds.")

    if not all_messages:
        print("No new messages found in the specified time window to process for rankings.")
        return {"statusCode": 200, "body": "No new messages to process."}

    chatter_message_counts = Counter()
    for item in all_messages:
        chatter_user_id = item.get("chatter_user_id", {}).get("S")
        if chatter_user_id:
            chatter_message_counts[chatter_user_id] += 1

    print(f"Aggregated chatter message counts: {chatter_message_counts}")

    top_chatters = chatter_message_counts.most_common(10)

    top_chatters_formatted = []
    for user_id, count in top_chatters:
        top_chatters_formatted.append({
            "M": {
                "user_id": {"S": user_id},
                "message_count": {"N": str(count)} # 'N' denotes a Number type
            }
        })

    try:
        dynamodb.put_item(
            TableName=rankings_table_name,
            Item={
                "ranking_type": {"S": "chatter_activity"},
                "window_end_unixtime": {"N": str(end_unixtime)},
                "window_start_unixtime": {"N": str(start_unixtime)},
                "top_chatters": {"L": top_chatters_formatted},
                "processed_at": {"N": str(end_unixtime)}
            }
        )
        print(f"Successfully updated rankings for window ending at {end_unixtime}")
    except Exception as e:
        print(f"Error writing to rankings table: {e}")
        return {"statusCode": 500, "body": f"Error writing rankings: {e}"}

    return {"statusCode": 200, "body": "Rankings processed successfully."}
