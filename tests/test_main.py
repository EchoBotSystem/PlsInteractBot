import hmac
import json
from unittest.mock import patch

from main import (
    MESSAGE_ID_KEY,
    MESSAGE_SIGNATURE_KEY,
    MESSAGE_TIMESTAMP_KEY,
    MESSAGE_TYPE_KEY,
    is_challenge,
    is_channel_chat_message,
    is_valid_event,
    is_valid_signature,
    lambda_handler,
    save_channel_chat_message,
)


def test_is_valid_event_valid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "signature",
            MESSAGE_TYPE_KEY: "type",
        },
        "body": "body",
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is True


def test_is_valid_event_missing_id():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "signature",
            MESSAGE_TYPE_KEY: "type",
        },
        "body": "body",
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is False


def test_is_valid_event_missing_timestamp():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_SIGNATURE_KEY: "signature",
            MESSAGE_TYPE_KEY: "type",
        },
        "body": "body",
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is False


def test_is_valid_event_missing_signature():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_TYPE_KEY: "type",
        },
        "body": "body",
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is False


def test_is_valid_event_missing_type():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "signature",
        },
        "body": "body",
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is False


def test_is_valid_event_missing_body():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "signature",
            MESSAGE_TYPE_KEY: "type",
        },
    }

    # Act
    result = is_valid_event(event)

    # Assert
    assert result is False


def test_is_valid_signature_valid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "signature",
        },
        "body": "body",
    }
    with patch("main.get_secret", return_value=b"test_secret"):
        message_id = event["headers"][MESSAGE_ID_KEY]
        message_timestamp = event["headers"][MESSAGE_TIMESTAMP_KEY]
        message_body = event["body"]
        secret = b"test_secret"
        expected_signature = (
            "sha256="
            + hmac.digest(
                secret,
                (message_id + message_timestamp + message_body).encode("utf-8"),
                "sha256",
            ).hex()
        )
        event["headers"][MESSAGE_SIGNATURE_KEY] = expected_signature
        # Act
        result = is_valid_signature(event)

        # Assert
        assert result is True


def test_is_valid_signature_invalid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=invalid_signature",
        },
        "body": "body",
    }
    with patch("main.get_secret", return_value=b"test_secret"):
        # Act
        result = is_valid_signature(event)

        # Assert
        assert result is False


def test_is_challenge_valid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "webhook_callback_verification",
        },
    }

    # Act
    result = is_challenge(event)

    # Assert
    assert result is True


def test_is_challenge_invalid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "notification",
        },
    }

    # Act
    result = is_challenge(event)

    # Assert
    assert result is False


def test_is_channel_chat_message_valid():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": json.dumps(
            {
                "subscription": {
                    "type": "channel.chat.message",
                    "condition": {"broadcaster_user_id": "123"},
                },
                "event": {
                    "message_id": "456",
                    "chatter_user_id": "789",
                    "message": {"text": "Hello"},
                },
            },
        ),
    }

    # Act
    result = is_channel_chat_message(event)

    # Assert
    assert result is True


def test_is_channel_chat_message_invalid_type():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "webhook_callback_verification",
        },
        "body": json.dumps(
            {
                "subscription": {
                    "type": "channel.chat.message",
                    "condition": {"broadcaster_user_id": "123"},
                },
                "event": {
                    "message_id": "456",
                    "chatter_user_id": "789",
                    "message": {"text": "Hello"},
                },
            },
        ),
    }

    # Act
    result = is_channel_chat_message(event)

    # Assert
    assert result is False


def test_is_channel_chat_message_missing_body():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "notification",
        },
    }

    # Act
    result = is_channel_chat_message(event)

    # Assert
    assert result is False


def test_is_channel_chat_message_invalid_json_body():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": "this is not json",
    }

    # Act
    result = is_channel_chat_message(event)

    # Assert
    assert result is False


def test_is_channel_chat_message_invalid_subscription_type():
    # Arrange
    event = {
        "headers": {
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": json.dumps(
            {
                "subscription": {
                    "type": "channel.follow",  # Incorrect type
                    "condition": {"broadcaster_user_id": "123"},
                },
                "event": {
                    "user_id": "789",
                },
            },
        ),
    }

    # Act
    result = is_channel_chat_message(event)

    # Assert
    assert result is False


@patch("main.boto3.client")
def test_save_channel_chat_message(mock_dynamodb_client):
    # Arrange
    event = {
        "body": json.dumps(
            {
                "subscription": {"condition": {"broadcaster_user_id": "123"}},
                "event": {
                    "message_id": "456",
                    "chatter_user_id": "789",
                    "message": {"text": "Hello"},
                },
            },
        ),
        "requestContext": {"timeEpoch": 1678886400},
    }

    # Act
    save_channel_chat_message(event)

    # Assert
    mock_dynamodb_client.return_value.put_item.assert_called_once_with(
        TableName="comments",
        Item={
            "message_id": {"S": "456"},
            "chatter_user_id": {"S": "789"},
            "broadcaster_user_id": {"S": "123"},
            "message_content": {"S": "Hello"},
            "reception_unixtime": {"N": "1678886400"},
        },
    )


def test_lambda_handler_valid_event_valid_signature_challenge():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "webhook_callback_verification",
        },
        "body": json.dumps({"challenge": "test_challenge"}),
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=True),
        patch("main.is_challenge", return_value=True),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        assert result["body"] == "test_challenge"


def test_lambda_handler_invalid_event():
    # Arrange
    event = {}
    context = {}
    with patch("main.is_valid_event", return_value=False):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 400


def test_lambda_handler_invalid_signature():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": "{}",
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=False),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 403


def test_lambda_handler_challenge_no_challenge_in_body():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "webhook_callback_verification",
        },
        "body": json.dumps({}),  # Missing "challenge" key
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=True),
        patch("main.is_challenge", return_value=True),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 400
        assert result["body"] == "No challenge found"


@patch("main.save_channel_chat_message")
def test_lambda_handler_channel_chat_message_success(mock_save_channel_chat_message):
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": json.dumps(
            {"subscription": {"type": "channel.chat.message"}, "event": {"message_id": "123"}},
        ),
        "requestContext": {"timeEpoch": 1234567890},
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=True),
        patch("main.is_challenge", return_value=False),
        patch("main.is_channel_chat_message", return_value=True),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
        mock_save_channel_chat_message.assert_called_once_with(event)


@patch("main.save_channel_chat_message", side_effect=Exception("DynamoDB error"))
def test_lambda_handler_channel_chat_message_exception(mock_save_channel_chat_message):
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "notification",
        },
        "body": json.dumps(
            {"subscription": {"type": "channel.chat.message"}, "event": {"message_id": "123"}},
        ),
        "requestContext": {"timeEpoch": 1234567890},
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=True),
        patch("main.is_challenge", return_value=False),
        patch("main.is_channel_chat_message", return_value=True),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200  # Still returns 200 to prevent retries
        mock_save_channel_chat_message.assert_called_once_with(event)


def test_lambda_handler_other_valid_event():
    # Arrange
    event = {
        "headers": {
            MESSAGE_ID_KEY: "id",
            MESSAGE_TIMESTAMP_KEY: "timestamp",
            MESSAGE_SIGNATURE_KEY: "sha256=signature",
            MESSAGE_TYPE_KEY: "channel.follow",  # Some other valid type
        },
        "body": json.dumps({"event_data": "some_data"}),
    }
    context = {}
    with (
        patch("main.is_valid_event", return_value=True),
        patch("main.is_valid_signature", return_value=True),
        patch("main.is_challenge", return_value=False),
        patch("main.is_channel_chat_message", return_value=False),
    ):
        # Act
        result = lambda_handler(event, context)

        # Assert
        assert result["statusCode"] == 200
