import json
from unittest.mock import MagicMock

import pytest

from broadcaster import get_api_gateway_endpoint, lambda_handler


@pytest.fixture
def mock_boto3_clients(mocker):
    """Mocks boto3 clients (DynamoDB and APIGatewayManagementAPI)."""
    mock_dynamodb_resource = mocker.patch("boto3.resource")
    mock_connections_table = MagicMock()
    mock_dynamodb_resource.return_value.Table.return_value = mock_connections_table

    mock_apigw_client = mocker.patch("boto3.client")
    mock_apigw_client.return_value = MagicMock()

    # Mock the GoneException for apigw client
    mock_apigw_client.return_value.exceptions.GoneException = type("GoneException", (Exception,), {})

    return mock_dynamodb_resource, mock_connections_table, mock_apigw_client.return_value


@pytest.fixture
def mock_commons_get_ranking(mocker):
    """Mocks commons.get_ranking."""
    return mocker.patch("commons.get_ranking")


def test_lambda_handler_success_multiple_connections(mock_boto3_clients, mock_commons_get_ranking, mocker):
    """
    Tests that lambda_handler successfully broadcasts ranking to multiple connected clients.
    """
    mock_dynamodb_resource, mock_connections_table, mock_apigw_client = mock_boto3_clients

    mocker.patch.dict(
        "os.environ",
        {"DOMAIN": "example.com", "STAGE": "prod"},
    )
    mock_connections_table.scan.return_value = {
        "Items": [
            {"connection_id": "conn1"},
            {"connection_id": "conn2"},
        ],
    }
    mock_commons_get_ranking.return_value = {"user1": 100, "user2": 90}

    response = lambda_handler({}, {})

    mock_connections_table.scan.assert_called_once()
    mock_commons_get_ranking.assert_called_once()
    mock_apigw_client.post_to_connection.assert_has_calls(
        [
            mocker.call(
                ConnectionId="conn1",
                Data=json.dumps({"type": "ranking", "data": {"user1": 100, "user2": 90}}).encode("utf-8"),
            ),
            mocker.call(
                ConnectionId="conn2",
                Data=json.dumps({"type": "ranking", "data": {"user1": 100, "user2": 90}}).encode("utf-8"),
            ),
        ],
        any_order=True,
    )
    mock_connections_table.delete_item.assert_not_called()
    assert response == {"statusCode": 200, "body": "Ranking sent to clients"}


def test_lambda_handler_no_connections(mock_boto3_clients, mock_commons_get_ranking, mocker):
    """
    Tests that lambda_handler behaves correctly when there are no active connections.
    """
    mock_dynamodb_resource, mock_connections_table, mock_apigw_client = mock_boto3_clients

    mocker.patch.dict(
        "os.environ",
        {"DOMAIN": "example.com", "STAGE": "prod"},
    )
    mock_connections_table.scan.return_value = {"Items": []}
    mock_commons_get_ranking.return_value = {"user1": 100, "user2": 90}

    response = lambda_handler({}, {})

    mock_connections_table.scan.assert_called_once()
    mock_commons_get_ranking.assert_called_once()
    mock_apigw_client.post_to_connection.assert_not_called()
    mock_connections_table.delete_item.assert_not_called()
    assert response == {"statusCode": 200, "body": "Ranking sent to clients"}


def test_lambda_handler_gone_exception(mock_boto3_clients, mock_commons_get_ranking, mocker):
    """
    Tests that lambda_handler correctly handles GoneException by deleting the connection.
    """
    mock_dynamodb_resource, mock_connections_table, mock_apigw_client = mock_boto3_clients

    mocker.patch.dict(
        "os.environ",
        {"DOMAIN": "example.com", "STAGE": "prod"},
    )
    mock_connections_table.scan.return_value = {
        "Items": [
            {"connection_id": "conn_gone"},
            {"connection_id": "conn_ok"},
        ],
    }
    mock_commons_get_ranking.return_value = {"user1": 100}

    # Configure post_to_connection to raise GoneException for 'conn_gone'
    def post_to_connection_side_effect(**kwargs: dict):
        if kwargs["ConnectionId"] == "conn_gone":
            raise mock_apigw_client.exceptions.GoneException("Connection gone")
        # For 'conn_ok', do nothing (simulate success)

    mock_apigw_client.post_to_connection.side_effect = post_to_connection_side_effect

    response = lambda_handler({}, {})

    mock_connections_table.scan.assert_called_once()
    mock_commons_get_ranking.assert_called_once()
    assert mock_apigw_client.post_to_connection.call_count == 2
    mock_connections_table.delete_item.assert_called_once_with(Key={"connection_id": "conn_gone"})
    assert response == {"statusCode": 200, "body": "Ranking sent to clients"}


def test_lambda_handler_other_exception_propagates(mock_boto3_clients, mock_commons_get_ranking, mocker):
    """
    Tests that lambda_handler allows non-GoneException errors during post_to_connection to propagate.
    """
    mock_dynamodb_resource, mock_connections_table, mock_apigw_client = mock_boto3_clients

    mocker.patch.dict(
        "os.environ",
        {"DOMAIN": "example.com", "STAGE": "prod"},
    )
    mock_connections_table.scan.return_value = {
        "Items": [
            {"connection_id": "conn_error"},
        ],
    }
    mock_commons_get_ranking.return_value = {"user1": 100}

    mock_apigw_client.post_to_connection.side_effect = Exception("Some other error")

    with pytest.raises(Exception, match="Some other error"):
        lambda_handler({}, {})

    mock_connections_table.scan.assert_called_once()
    mock_commons_get_ranking.assert_called_once()
    mock_apigw_client.post_to_connection.assert_called_once()
    mock_connections_table.delete_item.assert_not_called()


def test_get_api_gateway_endpoint_success(mocker):
    """
    Tests that get_api_gateway_endpoint correctly constructs the URL
    when DOMAIN and STAGE environment variables are set.
    """
    mocker.patch.dict(
        "os.environ",
        {"DOMAIN": "example.com", "STAGE": "prod"},
    )
    expected_url = "https://example.com/prod"
    assert get_api_gateway_endpoint() == expected_url


def test_get_api_gateway_endpoint_missing_env_var(mocker):
    """
    Tests that get_api_gateway_endpoint raises KeyError if DOMAIN or STAGE
    environment variables are missing.
    """
    # Patch os.environ to be empty for this test
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(KeyError):
        get_api_gateway_endpoint()
