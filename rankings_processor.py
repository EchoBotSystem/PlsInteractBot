import json
import time
from collections import Counter

import boto3

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

    connection_id = None
    route_key = None

    try:
        request_context = event.get("requestContext")

        if request_context:
            connection_id = request_context.get("connectionId")
            route_key = request_context.get("routeKey")

            # Validate essential WebSocket context keys
            if connection_id is None or route_key is None:
                print("Warning: Malformed WebSocket event (missing connectionId or routeKey).")
                return {"statusCode": 400, "body": "Bad Request: Malformed WebSocket event."}

            # Initialize apig_management client if it's a WebSocket event
            if not apig_management:
                domain_name = request_context.get("domainName")
                stage = request_context.get("stage")
                if not domain_name or not stage:
                    print("Error: Missing domainName or stage in requestContext for WebSocket.")
                    return {"statusCode": 500, "body": "Internal Server Error: WebSocket endpoint details missing."}
                endpoint = f"https://{domain_name}/{stage}"
                apig_management = boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)

            print(f"event: {event}")

            if route_key == "$connect":
                return {"statusCode": 200}
            elif route_key == "getRanking":
                print("Enter in route getRanking")
                return get_ranking(event, connection_id)
            elif route_key == "$disconnect":
                return {"statusCode": 200}
            else:  # Unknown WebSocket route key
                print(f"Unknown route key: {route_key}")
                return {"statusCode": 400, "body": f"Unknown route key: {route_key}"}
        else:
            # Not a WebSocket event, assume direct invocation
            print("Direct invocation for ranking processing.")
            return get_ranking(event, connection_id) # connection_id will be None for direct invocations

    except Exception as e:
        print(f"An unexpected error occurred in lambda_handler: {e}")
        return {"statusCode": 500, "body": f"Internal server error: {e}"}


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

    status_code = 200
    body_message = ""
    response_type = "ranking"
    response_data = {"topChatters": []}

    try:
        # Determine the time window for processing.
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
            body_message = "No new messages to process."
            # response_data already has empty topChatters
        else:
            # Aggregate message counts per chatter
            chatter_message_counts = Counter()
            for item in all_messages:
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
            response_data["topChatters"] = top_chatters_response  # Update response_data

            try:
                dynamodb.put_item(
                    TableName="rankings",
                    Item={
                        "ranking_type": {"S": "chatter_activity"},
                        "window_end_unixtime": {"N": str(end_unixtime)},
                        "top_chatters": {"L": top_chatters_formatted},
                    },
                )
                print("Rankings saved successfully to DynamoDB.")
                body_message = "Rankings processed successfully."
            except Exception as e:
                print(f"Error writing rankings to DynamoDB: {e}")
                status_code = 500
                body_message = f"Error writing rankings: {e}"
                response_type = "error"
                response_data = {"message": body_message}  # Update response_data for error

    except Exception as e:
        # Catch any other unexpected errors during ranking processing (e.g., DynamoDB scan errors)
        print(f"An error occurred during ranking processing: {e}")
        status_code = 500
        body_message = f"Error processing rankings: {e}"
        response_type = "error"
        response_data = {"message": body_message}

    # Send message back to WebSocket client if connection_id is provided
    if connection_id:
        message = {"type": response_type, "data": response_data}
        try:
            apig_management.post_to_connection(
                Data=json.dumps(message).encode("utf-8"),
                ConnectionId=connection_id,
            )
        except Exception as e:
            print(f"Error posting to connection {connection_id}: {e}")
            # Log the error but don't change the main Lambda response status
            # as the primary task (ranking processing/error handling) is done.

    return {"statusCode": status_code, "body": body_message}
