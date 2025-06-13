import json

import aws
import commons

aws.init_dynamodb()
aws.init_connections_table()
aws.init_apigw()


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
    ranking_data = commons.get_ranking()
    connections = aws.connections_table.scan().get("Items", [])
    for connection in connections:
        connection_id = connection["connection_id"]
        try:
            aws.apigw.post_to_connection(
                ConnectionId=connection_id,
                Data=json.dumps({"type": "ranking", "data": ranking_data}).encode(
                    "utf-8"
                ),
            )
        except aws.apigw.exceptions.GoneException:
            aws.connections_table.delete_item(Key={"connection_id": connection_id})
    return {"statusCode": 200, "body": "Ranking sent to clients"}
