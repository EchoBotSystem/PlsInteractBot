import boto3
import json
import os
import commons


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    connections_table = dynamodb.Table("web_socket_sonnections")

    apigw = boto3.client(
        "apigatewaymanagementapi", endpoint_url=get_api_gateway_endpoint()
    )

    connections = connections_table.scan().get("Items", [])
    for connection in connections:
        connection_id = connection["connection_id"]
        try:
            apigw.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(
                    {"type": "ranking", "data": commons.get_ranking()}
                ).encode("utf-8"),
            )
        except apigw.exceptions.GoneException:
            connections_table.delete_item(Key={"connection_id": connection_id})
    return {"statusCode": 200, "body": "Ranking sent to clients"}


def get_api_gateway_endpoint() -> str:
    return f"https://{os.environ.get('DOMAIN')}/{os.environ.get('STAGE')}"
