import datetime
import json
from unittest.mock import MagicMock

import pytest
import urllib3  # Import urllib3 for exception handling

from commons import get_client_id, get_oauth_token, get_user


@pytest.fixture
def mock_boto3_clients(mocker):
    """Mocks boto3 clients (DynamoDB) for tests."""
    mock_dynamodb = MagicMock()
    mocker.patch("commons.boto3.client", return_value=mock_dynamodb)
    return mock_dynamodb


@pytest.fixture
def mock_urllib3_pool_manager(mocker):
    """Mocks urllib3.PoolManager for tests."""
    mock_pool_manager = MagicMock()
    mocker.patch("commons.urllib3.PoolManager", return_value=mock_pool_manager)
    return mock_pool_manager


def test_get_user_from_dynamodb_cache(mock_boto3_clients, mock_urllib3_pool_manager):
    """
    Test that get_user retrieves user data from DynamoDB cache if available.
    """
    user_id = "12345"
    expected_login = "testuser"
    expected_profile_image_url = "http://example.com/profile.png"

    mock_boto3_clients.get_item.return_value = {
        "Item": {
            "user_id": {"S": user_id},
            "login": {"S": expected_login},
            "profile_image_url": {"S": expected_profile_image_url},
        },
    }

    user = get_user(user_id)

    assert user.id == user_id
    assert user.login == expected_login
    assert user.profile_image_url == expected_profile_image_url
    mock_boto3_clients.get_item.assert_called_once_with(
        TableName="users",
        Key={"user_id": {"S": user_id}},
    )
    mock_urllib3_pool_manager.request.assert_not_called()


def test_get_user_from_twitch_api_and_cache(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
    mocker,
):
    """
    Test that get_user fetches user data from Twitch API if not in DynamoDB
    and then caches it.
    """
    user_id = "67890"
    expected_login = "newuser"
    expected_profile_image_url = "http://example.com/new_profile.png"

    mock_boto3_clients.get_item.return_value = {}  # Not found in DynamoDB

    # Prepare the JSON response as bytes
    json_response_str = json.dumps(
        {
            "data": [
                {
                    "id": user_id,
                    "login": expected_login,
                    "profile_image_url": expected_profile_image_url,
                },
            ],
        },
    )
    mock_http_response = MagicMock()
    # Mock the .decode() method on the 'data' attribute to return the string
    mock_http_response.data.decode.return_value = json_response_str

    # Correctly set the return value for the mocked request method
    mock_urllib3_pool_manager.request.return_value = mock_http_response

    # Mock environment variables for Twitch API calls
    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("TWITCH_CLIENT_ID", "mock_client_id")
        mp.setenv("TWITCH_OAUTH_TOKEN", "mock_oauth_token")

        # Mock datetime.datetime.now() to control expireAt
        mock_now = mocker.patch("commons.datetime.datetime")
        mock_now.now.return_value = datetime.datetime(2025, 1, 1, 12, 0, 0)
        mock_now.timedelta = datetime.timedelta  # Keep timedelta original

        user = get_user(user_id)

        assert user.id == user_id
        assert user.login == expected_login
        assert user.profile_image_url == expected_profile_image_url

        mock_boto3_clients.get_item.assert_called_once_with(
            TableName="users",
            Key={"user_id": {"S": user_id}},
        )
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.put_item.assert_called_once_with(
            TableName="users",
            Item={
                "user_id": {"S": user_id},
                "login": {"S": expected_login},
                "profile_image_url": {"S": expected_profile_image_url},
                "expireAt": {"N": str(int(datetime.datetime(2025, 2, 1, 12, 0, 0).timestamp()))},
            },
        )


def test_get_user_not_found_in_twitch_api(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
    mocker,
):
    """
    Test that get_user raises ValueError if user not found in Twitch API
    and caches the error.
    """
    user_id = "99999"

    mock_boto3_clients.get_item.return_value = {}  # Not found in DynamoDB

    # Prepare the JSON response as bytes
    json_response_str = json.dumps(
        {"data": []},
    )  # User not found in Twitch
    mock_http_response = MagicMock()
    # Mock the .decode() method on the 'data' attribute to return the string
    mock_http_response.data.decode.return_value = json_response_str

    # Correctly set the return value for the mocked request method
    mock_urllib3_pool_manager.request.return_value = mock_http_response

    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("TWITCH_CLIENT_ID", "mock_client_id")
        mp.setenv("TWITCH_OAUTH_TOKEN", "mock_oauth_token")

        # Mock datetime.datetime.now() to control expireAt
        mock_now = mocker.patch("commons.datetime.datetime")
        mock_now.now.return_value = datetime.datetime(2025, 1, 1, 12, 0, 0)
        mock_now.timedelta = datetime.timedelta  # Keep timedelta original

        with pytest.raises(ValueError, match=f"User {user_id} not found in Twitch API"):
            get_user(user_id)

        mock_boto3_clients.get_item.assert_called_once()
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.put_item.assert_called_once_with(
            TableName="users",
            Item={
                "user_id": {"S": user_id},
                "error_twitch_api": {"S": "f"},
                "expireAt": {"N": str(int(datetime.datetime(2025, 2, 1, 12, 0, 0).timestamp()))},
            },
        )


def test_get_user_dynamodb_cached_error(mock_boto3_clients, mock_urllib3_pool_manager):
    """
    Test that get_user raises ValueError if user is found in DynamoDB
    but marked as an error from a previous Twitch API lookup.
    """
    user_id = "54321"
    mock_boto3_clients.get_item.return_value = {
        "Item": {
            "user_id": {"S": user_id},
            "error_twitch_api": {"S": "f"},
        },
    }

    with pytest.raises(ValueError, match=f"User {user_id} not found in Twitch API"):
        get_user(user_id)

    mock_boto3_clients.get_item.assert_called_once()
    mock_urllib3_pool_manager.request.assert_not_called()


def test_get_user_twitch_api_http_error(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
):
    """
    Test that get_user handles HTTP errors from Twitch API gracefully.
    """
    user_id = "11223"
    user_id = "11223"  # Redefine user_id for clarity in this test
    mock_boto3_clients.get_item.return_value = {}  # Not found in DynamoDB

    # Correctly set the side_effect for the mocked request method
    mock_urllib3_pool_manager.request.side_effect = urllib3.exceptions.MaxRetryError(
        None,
        "http://example.com",
        "Connection refused",
    )

    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("TWITCH_CLIENT_ID", "mock_client_id")
        mp.setenv("TWITCH_OAUTH_TOKEN", "mock_oauth_token")

        with pytest.raises(urllib3.exceptions.MaxRetryError):
            get_user(user_id)

        mock_boto3_clients.get_item.assert_called_once()
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.put_item.assert_not_called()  # Should not cache on API error


def test_get_client_id():
    """
    Test that get_client_id correctly retrieves the TWITCH_CLIENT_ID
    environment variable.
    """
    expected_client_id = "test_client_id_456"
    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("TWITCH_CLIENT_ID", expected_client_id)
        assert get_client_id() == expected_client_id

    with pytest.MonkeyPatch().context() as mp:
        mp.delenv("TWITCH_CLIENT_ID", raising=False)
        assert get_client_id() is None


def test_get_oauth_token():
    """
    Test that get_oauth_token correctly retrieves the TWITCH_OAUTH_TOKEN
    environment variable.
    """
    expected_token = "test_oauth_token_123"
    with pytest.MonkeyPatch().context() as mp:
        mp.setenv("TWITCH_OAUTH_TOKEN", expected_token)
        assert get_oauth_token() == expected_token

    with pytest.MonkeyPatch().context() as mp:
        mp.delenv("TWITCH_OAUTH_TOKEN", raising=False)
        assert get_oauth_token() is None
