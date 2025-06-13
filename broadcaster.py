import json
import os

import boto3

import commons


def lambda_handler(event: dict, contex: dict) -> dict:
    """
    Lambda handler for broadcasting ranking updates to connected WebSocket clients.

    It scans the connections table, retrieves the latest ranking, and sends it
    to each connected client. If a connection is no longer valid (GoneException),
    it removes the connection from the table.

    Args:
        event (dict): The event dictionary from AWS Lambda, containing information
                      about the invocation.
        contex (dict): The context dictionary from AWS Lambda, providing runtime
                       information.

    Returns:
        dict: A dictionary with `statusCode` and `body` indicating the result of the operation.

    Raises:
        apigw.exceptions.GoneException: If a WebSocket connection is no longer valid,
                                        it is caught and the connection is removed.
    """
    dynamodb = boto3.resource("dynamodb")
    connections_table = dynamodb.Table("web_socket_sonnections")

    apigw = boto3.client("apigatewaymanagementapi", endpoint_url=get_api_gateway_endpoint())

    connections = connections_table.scan().get("Items", [])
    for connection in connections:
        connection_id = connection["connection_id"]
        try:
            apigw.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps({"type": "ranking", "data": commons.get_ranking()}).encode("utf-8"),
            )
        except apigw.exceptions.GoneException:
            connections_table.delete_item(Key={"connection_id": connection_id})
    return {"statusCode": 200, "body": "Ranking sent to clients"}


def get_api_gateway_endpoint() -> str:
    """
    Constructs the API Gateway management endpoint URL.

    This URL is used to post messages back to connected WebSocket clients.
    It relies on 'DOMAIN' and 'STAGE' environment variables.

    Returns:
        str: The constructed API Gateway management endpoint URL.

    Raises:
        KeyError: If 'DOMAIN' or 'STAGE' environment variables are not set.
    """
    return f"https://{os.environ.get('DOMAIN')}/{os.environ.get('STAGE')}"
