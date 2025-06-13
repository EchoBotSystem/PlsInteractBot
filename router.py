import json

import boto3

import commons


def lambda_handler(event: dict, context: dict) -> dict:
    route_key = event["requestContext"]["routeKey"]
    connection_id = event["requestContext"]["connectionId"]
    domain_name = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]

    apig_management = boto3.client("apigatewaymanagementapi", endpoint_url=f"https://{domain_name}/{stage}")

    dynamodb = boto3.resource("dynamodb")
    connections_table = dynamodb.Table("web_socket_sonnections")

    if route_key == "$connect":
        connections_table.put_item(Item={"connection_id": connection_id})
        return {"statusCode": 200}

    if route_key == "$disconnect":
        connections_table.delete_item(Key={"connection_id": connection_id})
        return {"statusCode": 200}

    if route_key == "getRanking":
        try:
            apig_management.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps({"type": "ranking", "data": commons.get_ranking()}).encode("utf-8"),
            )
        except Exception as e:
            print("Error:", e)
            return {"statusCode": 500, "body": str(e)}

        return {"statusCode": 200}

    return {"statusCode": 400, "body": "Unknown route"}
