import json
import time
from collections import Counter

import boto3

# Initialize a DynamoDB client.
apig_management = None
comments_table_name = "comments"
dynamodb = None


def lambda_handler(event: dict, context: dict) -> dict:
    """
    The main entry point for the AWS Lambda function.
    Processes chat messages from the 'comments' DynamoDB table, aggregates
    chatter activity, and stores the top chatters in the 'rankings' table.

    Args:
        event (dict): The event dictionary, potentially containing 'end_unixtime'
                      to specify the end of the processing window.
        context (dict): The runtime context of the Lambda function.

    Returns:
        dict: A dictionary representing the HTTP response, indicating success or failure.
    """
    print("Rankings processor started.")
    global apig_management

    connection_id = None  # Initialize connection_id to None by default

    # Check if the event is from API Gateway (WebSocket)
    if "requestContext" in event and "connectionId" in event["requestContext"]:
        connection_id = event["requestContext"]["connectionId"]  # This is the string connection ID
        route_key = event["requestContext"]["routeKey"]

        if not apig_management:
            endpoint = f"https://{event['requestContext']['domainName']}/{event['requestContext']['stage']}"
            apig_management = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)
        print(f"event: {event}")  # Use f-string for better logging
        if route_key == "$connect":
            return {"statusCode": 200}
        if route_key == "getRanking":  # Use elif for clarity and efficiency
            print("Enter in route getRanking")
            # Pass event first, then connection_id
            return get_ranking(event, connection_id)
        if route_key == "$disconnect":  # Use elif
            return {"statusCode": 200}
        # Handle unknown WebSocket route keys
        print(f"Unknown route key: {route_key}")
        return {"statusCode": 400, "body": f"Unknown route key: {route_key}"}

    # Assume it's a direct invocation for ranking processing
    print("Direct invocation for ranking processing.")
    # For direct invocation, connection_id remains None
    return get_ranking(event, connection_id)


def get_ranking(event: dict, connection_id: str | None = None) -> dict:
    """
    Retrieves and processes chat messages from the 'comments' DynamoDB table,
    aggregates chatter activity, and stores the top chatters in the 'rankings' table.
    Args:
        event (dict): The event dictionary, potentially containing 'end_unixtime'
                      to specify the end of the processing window.
        connection_id (str, optional): The connection ID for the API Gateway management API, if applicable.
                                       Defaults to None for direct invocations.
    Returns:
        dict: A dictionary representing the HTTP response, indicating success or failure.
    """
    global dynamodb
    if not dynamodb:
        dynamodb = boto3.client("dynamodb")

    # Determine the time window for processing.
    # 'end_unixtime' can be provided in the event; otherwise, it defaults to the current time.
    # 'start_unixtime' is set to 30 days prior to 'end_unixtime'.
    end_unixtime = int(time.time() * 1000) if event.get("end_unixtime") is None else int(event["end_unixtime"])
    start_unixtime = end_unixtime - (60 * 60 * 24 * 30 * 1000)  # 30 days in milliseconds

    print(f"Processing messages from Unix time {start_unixtime} to {end_unixtime}")

    all_messages = []
    last_evaluated_key = None

    # Scan the 'comments' table to retrieve messages within the specified time window.
    # The scan operation is paginated, so a loop is used to retrieve all results.
    while True:
        scan_params = {
            "TableName": comments_table_name,
            # Only retrieve 'chatter_user_id' and 'reception_unixtime' to minimize read capacity units.
            "ProjectionExpression": "chatter_user_id, reception_unixtime",
            # Filter messages based on their reception timestamp.
            "FilterExpression": "reception_unixtime BETWEEN :start_time AND :end_time",
            "ExpressionAttributeValues": {
                ":start_time": {"N": str(start_unixtime)},  # 'N' denotes a Number type
                ":end_time": {"N": str(end_unixtime)},  # 'N' denotes a Number type
            },
        }
        # If a LastEvaluatedKey was returned in the previous response, use it to continue the scan.
        if last_evaluated_key:
            scan_params["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**scan_params)
        all_messages.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")

        # Break the loop if there are no more pages to scan.
        if not last_evaluated_key:
            break

    print(f"Found {len(all_messages)} messages in the last month seconds.")

    if not all_messages:
        print("No new messages found in the specified time window to process for rankings.")
        # If a connection_id is provided, send a message back to the client
        if connection_id:
            message = {"type": "ranking", "data": {"topChatters": []}}
            apig_management.post_to_connection(
                Data=json.dumps(message).encode("utf-8"),
                ConnectionId=connection_id,
            )
        return {"statusCode": 200, "body": "No new messages to process."}

    # Aggregate message counts per chatter using collections.Counter.
    chatter_message_counts = Counter()
    for item in all_messages:
        # Extract the chatter_user_id from the DynamoDB item format.
        chatter_user_id = item.get("chatter_user_id", {}).get("S")
        if chatter_user_id:
            chatter_message_counts[chatter_user_id] += 1

    print(f"Aggregated chatter message counts: {chatter_message_counts}")

    # Get the top 10 chatters based on message count.
    top_chatters = chatter_message_counts.most_common(10)

    # Format the top chatters data for storage in DynamoDB's List ('L') type.
    top_chatters_formatted = []
    top_chatters_response = []
    for user_id, count in top_chatters:
        top_chatters_formatted.append(
            {
                "M": {  # 'M' denotes a Map type
                    "user_id": {"S": user_id},  # 'S' denotes a String type
                    "message_count": {"N": str(count)},  # 'N' denotes a Number type
                },
            },
        )
        top_chatters_response.append(
            {
                "userId": user_id,
                "messageCount": count,
            },
        )

    # Only post to connection if a connection_id is provided (i.e., it's a WebSocket request)
    if connection_id:
        message = {"type": "ranking", "data": {"topChatters": top_chatters_response}}
        apig_management.post_to_connection(
            Data=json.dumps(message).encode("utf-8"),
            ConnectionId=connection_id,
        )
    return {"statusCode": 200, "body": "Rankings processed successfully."}
