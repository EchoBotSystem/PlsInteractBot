import boto3
import time

from collections import Counter
from boto3 import Session

def lambda_handler(event: dict, context: dict) -> dict:
    """
    AWS Lambda function to process recent chat messages and update a rankings table.
    This function is intended to be triggered periodically (e.g., every 30 seconds)
    by an EventBridge (CloudWatch Events) rule.
    """
    print("Rankings processor started.")

    dynamodb: Session = boto3.client("dynamodb")
    comments_table_name = "comments"
    rankings_table_name = "rankings" # Name of the DynamoDB table for rankings

    # Define the time window for messages to process (last 30 seconds)
    current_unixtime = int(time.time())
    # Calculate the start of the 30-second window
    start_unixtime = current_unixtime - 30

    print(f"Processing messages from Unix time {start_unixtime} to {current_unixtime}")

    all_messages = []
    last_evaluated_key = None

    # Scan the 'comments' table for messages within the defined time window.
    # Note: For large tables, a Scan operation can be inefficient as it reads
    # all items and then filters. For better performance on time-based queries,
    # consider adding a Global Secondary Index (GSI) on 'reception_unixtime'
    # to the 'comments' table, or designing the table with a composite key
    # that includes a time component.
    while True:
        scan_params = {
            "TableName": comments_table_name,
            "FilterExpression": "reception_unixtime BETWEEN :start_time AND :end_time",
            "ExpressionAttributeValues": {
                ":start_time": {"N": str(start_unixtime)},
                ":end_time": {"N": str(current_unixtime)},
            },
        }
        if last_evaluated_key:
            scan_params["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_params)
        all_messages.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")

        if not last_evaluated_key:
            break # No more items to retrieve

    print(f"Found {len(all_messages)} messages in the last 30 seconds.")

    if not all_messages:
        print("No new messages found in the specified time window to process for rankings.")
        return {"statusCode": 200, "body": "No new messages to process."}

    # Aggregate messages by 'chatter_user_id' to count activity
    chatter_message_counts = Counter()
    for item in all_messages:
        # DynamoDB items return values as dictionaries with type keys (e.g., {"S": "value"})
        chatter_user_id = item.get("chatter_user_id", {}).get("S")
        if chatter_user_id:
            chatter_message_counts[chatter_user_id] += 1

    print(f"Aggregated chatter message counts: {chatter_message_counts}")

    # Get the top 10 most active chatters for this window
    top_chatters = chatter_message_counts.most_common(10)

    # Format the top chatters data for DynamoDB's List of Maps (L) type
    top_chatters_formatted = []
    for user_id, count in top_chatters:
        top_chatters_formatted.append({
            "M": { # 'M' denotes a Map (dictionary) type in DynamoDB
                "user_id": {"S": user_id},
                "message_count": {"N": str(count)} # 'N' denotes a Number type
            }
        })

    # Store the generated ranking in the 'rankings' DynamoDB table.
    # The table should have 'ranking_type' as its Partition Key (String)
    # and 'window_end_unixtime' as its Sort Key (Number) to allow for
    # querying specific ranking types and retrieving the latest ranking.
    try:
        dynamodb.put_item(
            TableName=rankings_table_name,
            Item={
                "ranking_type": {"S": "chatter_activity"}, # Partition Key: Type of ranking
                "window_end_unixtime": {"N": str(current_unixtime)}, # Sort Key: End time of the window
                "window_start_unixtime": {"N": str(start_unixtime)}, # Start time of the window
                "top_chatters": {"L": top_chatters_formatted}, # List of top chatters
                "processed_at": {"N": str(current_unixtime)} # Timestamp when this ranking was generated
            }
        )
        print(f"Successfully updated rankings for window ending at {current_unixtime}")
    except Exception as e:
        print(f"Error writing to rankings table: {e}")
        # Return 500 to indicate an internal server error during processing
        return {"statusCode": 500, "body": f"Error writing rankings: {e}"}

    return {"statusCode": 200, "body": "Rankings processed successfully."}
