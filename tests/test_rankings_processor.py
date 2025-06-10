import time
from unittest.mock import Mock, patch

from rankings_processor import lambda_handler


def test_lambda_handler_success():
    # Arrange
    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client = Mock()
    with patch("rankings_processor.dynamodb", new=mock_dynamodb_client):
        mock_dynamodb_client.scan.return_value = {
            "Items": [
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 2000)}},
                {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 3000)}},
            ],
        }

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        assert result["body"] == "Rankings processed successfully."
        mock_dynamodb_client.put_item.assert_called_once()
        args, kwargs = mock_dynamodb_client.put_item.call_args
        assert kwargs["TableName"] == "rankings"
        item = kwargs["Item"]
        assert item["ranking_type"]["S"] == "chatter_activity"
        assert item["window_end_unixtime"]["N"] == str(event["end_unixtime"])


def test_lambda_handler_no_new_messages():
    # Arrange
    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client = Mock()
    with patch("rankings_processor.dynamodb", new=mock_dynamodb_client):
        mock_dynamodb_client.scan.return_value = {"Items": []}

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        assert result["body"] == "No new messages to process."
        mock_dynamodb_client.put_item.assert_not_called()


def test_lambda_handler_dynamodb_write_error():
    # Arrange
    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client = Mock()
    with patch("rankings_processor.dynamodb", new=mock_dynamodb_client):
        mock_dynamodb_client.scan.return_value = {
            "Items": [
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
            ],
        }
        mock_dynamodb_client.put_item.side_effect = Exception("DynamoDB write error")

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 500
        assert "Error writing rankings" in result["body"]


def test_lambda_handler_missing_end_unixtime():
    # Arrange
    event = {}
    context = {}
    mock_dynamodb_client = Mock()
    with patch("rankings_processor.dynamodb", new=mock_dynamodb_client):
        mock_dynamodb_client.scan.return_value = {
            "Items": [
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(int(time.time() * 1000) - 1000)}},
            ],
        }

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        assert result["body"] == "Rankings processed successfully."
        mock_dynamodb_client.put_item.assert_called_once()


def test_lambda_handler_valid_and_invalid_data():
    # Arrange
    event = {"end_unixtime": int(time.time() * 1000)}
    context = {}
    mock_dynamodb_client = Mock()
    with patch("rankings_processor.dynamodb", new=mock_dynamodb_client):
        mock_dynamodb_client.scan.return_value = {
            "Items": [
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 1000)}},
                {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 2000)}},
                {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": str(event["end_unixtime"] - 3000)}},
                {"reception_unixtime": {"N": str(event["end_unixtime"] - 4000)}},  # Missing chatter_user_id
                {"chatter_user_id": {"S": "user3"}},  # Missing reception_unixtime
            ],
        }

        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        assert result["body"] == "Rankings processed successfully."
        mock_dynamodb_client.put_item.assert_called_once()
        args, kwargs = mock_dynamodb_client.put_item.call_args
        item = kwargs["Item"]
        top_chatters = item["top_chatters"]["L"]
        assert len(top_chatters) > 0
