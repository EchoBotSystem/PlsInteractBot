import json

import aws
import commons

aws.init_dynamodb()
aws.init_connections_table()
aws.init_apigw()


def lambda_handler(event: dict, context: dict) -> dict:
    route_key = event["requestContext"]["routeKey"]
    connection_id = event["requestContext"]["connectionId"]
    if route_key == "$connect":
        aws.connections_table.put_item(Item={"connection_id": connection_id})
        return {"statusCode": 200}
    if route_key == "$disconnect":
        aws.connections_table.delete_item(Key={"connection_id": connection_id})
        return {"statusCode": 200}
    if route_key == "getRanking":
        try:
            aws.apigw.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps(
                    {"type": "ranking", "data": commons.get_ranking()}
                ).encode("utf-8"),
            )
        except Exception as e:
            print("Error:", e)
            return {"statusCode": 500, "body": str(e)}
        return {"statusCode": 200}
    return {"statusCode": 400, "body": "Unknown route"}
