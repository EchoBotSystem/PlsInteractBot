import json
import time
from unittest.mock import MagicMock

import pytest

import rankings_processor


# Consolidated fixture to mock all boto3 clients used in rankings_processor.py
@pytest.fixture(autouse=True)
def mock_boto3_clients(mocker):
    """Mocks boto3 clients (DynamoDB and APIGatewayManagementAPI) and ensures global states are reset."""
    # Reset the global variables before each test
    rankings_processor.dynamodb = None
    rankings_processor.apig_management = None

    # Create mock clients
    mock_dynamodb = MagicMock(name="DynamoDB_Client_Mock")
    mock_apig_management = MagicMock(name="APIGatewayManagementAPI_Client_Mock")

    # Define a side_effect function for boto3.client
    def patched_boto3_client_factory(service_name, **kwargs: dict):
        """Factory function to return the appropriate mock client based on the service name."""
        if service_name == "dynamodb":
            return mock_dynamodb
        if service_name == "apigatewaymanagementapi":
            return mock_apig_management
        # Fallback for any other boto3.client calls not explicitly mocked
        return mocker.DEFAULT  # This will call the original boto3.client for other services

    # Patch boto3.client with our custom side_effect
    mocker.patch("boto3.client", side_effect=patched_boto3_client_factory)

    # Yield both mocks for tests to use
    yield mock_dynamodb, mock_apig_management


# Existing tests modified to use fixtures
def test_lambda_handler_success(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients

    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 2000)}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 3000)}},
        ],
    }

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "Rankings processed successfully."
    mock_dynamodb_client_fixture.put_item.assert_called_once()
    args, kwargs = mock_dynamodb_client_fixture.put_item.call_args
    assert kwargs["TableName"] == "rankings"
    item = kwargs["Item"]
    assert item["ranking_type"]["S"] == "chatter_activity"
    assert item["window_end_unixtime"]["N"] == str(event["end_unixtime"])
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()  # Should not be called for direct invocation


def test_lambda_handler_no_new_messages(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients

    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {"Items": []}

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "No new messages to process."
    mock_dynamodb_client_fixture.put_item.assert_not_called()
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


def test_lambda_handler_dynamodb_write_error(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients

    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
        ],
    }
    mock_dynamodb_client_fixture.put_item.side_effect = Exception("DynamoDB write error")

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 500
    assert "Error writing rankings" in result["body"]
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


def test_lambda_handler_missing_end_unixtime(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients

    event = {}
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(int(time.time() * 1000) - 1000)}},
        ],
    }

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "Rankings processed successfully."
    mock_dynamodb_client_fixture.put_item.assert_called_once()
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


def test_lambda_handler_valid_and_invalid_data(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients

    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 2000)}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 3000)}},
            {"reception_unixtime": {"N": str(event["end_unixtime"] - 4000)}},  # Missing chatter_user_id
            {"chatter_user_id": {"S": "user3"}},  # Missing reception_unixtime
        ],
    }

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "Rankings processed successfully."
    mock_dynamodb_client_fixture.put_item.assert_called_once()
    args, kwargs = mock_dynamodb_client_fixture.put_item.call_args
    item = kwargs["Item"]
    top_chatters = item["top_chatters"]["L"]
    assert len(top_chatters) > 0
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


# New tests for WebSocket paths
def test_lambda_handler_connect_route(mock_boto3_clients):
    _, mock_apig_management_client_fixture = mock_boto3_clients

    event = {
        "requestContext": {
            "connectionId": "test-connection-id",
            "routeKey": "$connect",
            "domainName": "example.com",
            "stage": "prod",
        },
    }
    context = {}
    result = rankings_processor.lambda_handler(event, context)
    assert result["statusCode"] == 200
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


def test_lambda_handler_disconnect_route(mock_boto3_clients):
    _, mock_apig_management_client_fixture = mock_boto3_clients

    event = {
        "requestContext": {
            "connectionId": "test-connection-id",
            "routeKey": "$disconnect",
            "domainName": "example.com",
            "stage": "prod",
        },
    }
    context = {}
    result = rankings_processor.lambda_handler(event, context)
    assert result["statusCode"] == 200
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()


def test_lambda_handler_get_ranking_route_success(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients
    connection_id = "test-connection-id-get-ranking"
    event = {
        "requestContext": {
            "connectionId": connection_id,
            "routeKey": "getRanking",
            "domainName": "example.com",
            "stage": "prod",
        },
        "end_unixtime": int(time.time() * 1000),
    }
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 2000)}},
        ],
    }

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "Rankings processed successfully."
    mock_dynamodb_client_fixture.put_item.assert_called_once()
    mock_apig_management_client_fixture.post_to_connection.assert_called_once()
    _, kwargs = mock_apig_management_client_fixture.post_to_connection.call_args  # Unpack args (empty) and kwargs
    assert kwargs["ConnectionId"] == connection_id
    sent_data = json.loads(kwargs["Data"].decode("utf-8"))  # Access Data from kwargs
    assert sent_data["type"] == "ranking"
    assert len(sent_data["data"]["topChatters"]) == 2


def test_lambda_handler_get_ranking_route_no_messages(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients
    connection_id = "test-connection-id-no-messages"
    event = {
        "requestContext": {
            "connectionId": connection_id,
            "routeKey": "getRanking",
            "domainName": "example.com",
            "stage": "prod",
        },
        "end_unixtime": int(time.time() * 1000),
    }
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {"Items": []}

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 200
    assert result["body"] == "No new messages to process."
    mock_dynamodb_client_fixture.put_item.assert_not_called()
    mock_apig_management_client_fixture.post_to_connection.assert_called_once()
    _, kwargs = mock_apig_management_client_fixture.post_to_connection.call_args  # Unpack args (empty) and kwargs
    assert kwargs["ConnectionId"] == connection_id
    sent_data = json.loads(kwargs["Data"].decode("utf-8"))  # Access Data from kwargs
    assert sent_data["type"] == "ranking"
    assert sent_data["data"]["topChatters"] == []


def test_lambda_handler_get_ranking_route_dynamodb_write_error(mock_boto3_clients):
    # Arrange
    mock_dynamodb_client_fixture, mock_apig_management_client_fixture = mock_boto3_clients
    connection_id = "test-connection-id-write-error"
    event = {
        "requestContext": {
            "connectionId": connection_id,
            "routeKey": "getRanking",
            "domainName": "example.com",
            "stage": "prod",
        },
        "end_unixtime": int(time.time() * 1000),
    }
    context = {}
    mock_dynamodb_client_fixture.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
        ],
    }
    mock_dynamodb_client_fixture.put_item.side_effect = Exception("DynamoDB write error")

    # Act
    result = rankings_processor.lambda_handler(event, context)

    # Assert
    assert result["statusCode"] == 500
    assert "Error writing rankings" in result["body"]
    mock_apig_management_client_fixture.post_to_connection.assert_called_once()
    _, kwargs = mock_apig_management_client_fixture.post_to_connection.call_args  # Unpack args (empty) and kwargs
    assert kwargs["ConnectionId"] == connection_id
    sent_data = json.loads(kwargs["Data"].decode("utf-8"))  # Access Data from kwargs
    assert sent_data["type"] == "error"
    assert "Error writing rankings" in sent_data["data"]["message"]


def test_lambda_handler_unknown_route_key(mock_boto3_clients):
    _, mock_apig_management_client_fixture = mock_boto3_clients

    event = {
        "requestContext": {
            "connectionId": "test-connection-id",
            "routeKey": "unknownRoute",
            "domainName": "example.com",
            "stage": "prod",
        },
    }
    context = {}
    result = rankings_processor.lambda_handler(event, context)
    assert result["statusCode"] == 400
    assert "Unknown route key: unknownRoute" in result["body"]
    mock_apig_management_client_fixture.post_to_connection.assert_not_called()
