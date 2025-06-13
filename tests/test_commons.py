import datetime
import json
from unittest.mock import MagicMock

import pytest
import urllib3

from commons import get_client_id, get_oauth_token, get_ranking, get_users


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


def test_get_ranking_success_with_data(mock_boto3_clients, mocker):
    """
    Test that get_ranking correctly aggregates messages and fetches user data.
    """
    # Mock DynamoDB scan to return chat messages
    mock_boto3_clients.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": "1000"}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": "1001"}},
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": "1002"}},
            {"chatter_user_id": {"S": "user3"}, "reception_unixtime": {"N": "1003"}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": "1004"}},
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": "1005"}},
        ],
        "Count": 6,
    }

    # Mock get_users to return predefined User objects
    mock_get_users = mocker.patch("commons.get_users")
    mock_get_users.return_value = {
        "user1": MagicMock(id="user1", login="user1_login", profile_image_url="url1"),
        "user2": MagicMock(id="user2", login="user2_login", profile_image_url="url2"),
        "user3": MagicMock(id="user3", login="user3_login", profile_image_url="url3"),
    }

    # Mock datetime.datetime.now() for default time range
    mock_now = mocker.patch("commons.time.time")
    mock_now.return_value = 2000 / 1000  # Current time in seconds

    ranking = get_ranking()

    assert len(ranking) == 3
    assert ranking[0]["userId"] == "user1"
    assert ranking[0]["messageCount"] == 3
    assert ranking[0]["userLogin"] == "user1_login"
    assert ranking[1]["userId"] == "user2"
    assert ranking[1]["messageCount"] == 2
    assert ranking[2]["userId"] == "user3"
    assert ranking[2]["messageCount"] == 1

    mock_boto3_clients.scan.assert_called_once()
    assert mock_get_users.call_count == 1


def test_get_ranking_no_messages(mock_boto3_clients, mocker):
    """
    Test that get_ranking returns an empty list when no messages are found.
    """
    mock_boto3_clients.scan.return_value = {"Items": [], "Count": 0}
    mock_get_users = mocker.patch("commons.get_users")
    mock_get_users.return_value = {}  # Ensure it returns an empty dict if called

    # Mock datetime.datetime.now() for default time range
    mock_now = mocker.patch("commons.time.time")
    mock_now.return_value = 2000 / 1000

    ranking = get_ranking()

    assert ranking == []
    mock_boto3_clients.scan.assert_called_once()
    # get_users should not be called if there are no messages
    mock_get_users.assert_not_called()


def test_get_ranking_user_not_found_graceful_handling(mock_boto3_clients, mocker):
    """
    Test that get_ranking handles ValueError from get_users gracefully
    and continues processing other users.
    """
    mock_boto3_clients.scan.return_value = {
        "Items": [
            {"chatter_user_id": {"S": "user1"}, "reception_unixtime": {"N": "1000"}},
            {"chatter_user_id": {"S": "user_error"}, "reception_unixtime": {"N": "1001"}},
            {"chatter_user_id": {"S": "user2"}, "reception_unixtime": {"N": "1002"}},
        ],
        "Count": 3,
    }

    mock_get_users = mocker.patch("commons.get_users")
    mock_get_users.return_value = {
        "user1": MagicMock(id="user1", login="user1_login", profile_image_url="url1"),
        "user2": MagicMock(id="user2", login="user2_login", profile_image_url="url2"),
        # user_error is intentionally not included, simulating not found
    }

    # Mock datetime.datetime.now() for default time range
    mock_now = mocker.patch("commons.time.time")
    mock_now.return_value = 2000 / 1000

    ranking = get_ranking()

    assert len(ranking) == 2
    assert ranking[0]["userId"] == "user1"
    assert ranking[1]["userId"] == "user2"
    # get_users is called once with the list of top chatters
    mock_get_users.assert_called_once_with(["user1", "user_error", "user2"])


def test_get_ranking_time_range(mock_boto3_clients, mocker):
    """
    Test that get_ranking uses the provided start_unixtime and end_unixtime.
    """
    start_time = 500
    end_time = 1500

    mock_boto3_clients.scan.return_value = {"Items": [], "Count": 0}
    mocker.patch("commons.get_users")

    get_ranking(start_unixtime=start_time, end_unixtime=end_time)

    mock_boto3_clients.scan.assert_called_once_with(
        TableName="comments",
        ProjectionExpression="chatter_user_id, reception_unixtime",
        FilterExpression="reception_unixtime BETWEEN :start_time AND :end_time",
        ExpressionAttributeValues={
            ":start_time": {"N": str(start_time)},
            ":end_time": {"N": str(end_time)},
        },
    )


def test_get_ranking_dynamodb_scan_pagination(mock_boto3_clients, mocker):
    """
    Test that get_ranking handles DynamoDB scan pagination correctly.
    """
    # First scan response with LastEvaluatedKey
    mock_boto3_clients.scan.side_effect = [
        {
            "Items": [{"chatter_user_id": {"S": "userA"}, "reception_unixtime": {"N": "1000"}}],
            "LastEvaluatedKey": {"chatter_user_id": {"S": "userA_key"}},
        },
        # Second scan response without LastEvaluatedKey
        {
            "Items": [{"chatter_user_id": {"S": "userB"}, "reception_unixtime": {"N": "1001"}}],
            "LastEvaluatedKey": None,
        },
    ]

    mock_get_users = mocker.patch("commons.get_users")
    mock_get_users.return_value = {
        "userA": MagicMock(id="userA", login="userA_login", profile_image_url="urlA"),
        "userB": MagicMock(id="userB", login="userB_login", profile_image_url="urlB"),
    }

    # Mock datetime.datetime.now() for default time range
    mock_now = mocker.patch("commons.time.time")
    mock_now.return_value = 2000 / 1000

    ranking = get_ranking()

    assert len(ranking) == 2
    assert ranking[0]["userId"] == "userA"
    assert ranking[1]["userId"] == "userB"

    # Assert that scan was called twice
    assert mock_boto3_clients.scan.call_count == 2
    # Assert the first call
    mock_boto3_clients.scan.assert_any_call(
        TableName="comments",
        ProjectionExpression="chatter_user_id, reception_unixtime",
        FilterExpression="reception_unixtime BETWEEN :start_time AND :end_time",
        ExpressionAttributeValues={
            ":start_time": {"N": str(int(mock_now.return_value * 1000) - (60 * 60 * 24 * 30 * 1000))},
            ":end_time": {"N": str(int(mock_now.return_value * 1000))},
        },
    )
    # Assert the second call with ExclusiveStartKey
    mock_boto3_clients.scan.assert_any_call(
        TableName="comments",
        ProjectionExpression="chatter_user_id, reception_unixtime",
        FilterExpression="reception_unixtime BETWEEN :start_time AND :end_time",
        ExpressionAttributeValues={
            ":start_time": {"N": str(int(mock_now.return_value * 1000) - (60 * 60 * 24 * 30 * 1000))},
            ":end_time": {"N": str(int(mock_now.return_value * 1000))},
        },
        ExclusiveStartKey={"chatter_user_id": {"S": "userA_key"}},
    )


def test_get_users_from_dynamodb_cache(mock_boto3_clients, mock_urllib3_pool_manager):
    """
    Test that get_users retrieves user data from DynamoDB cache if available.
    """
    user_id = "12345"
    expected_login = "testuser"
    expected_profile_image_url = "http://example.com/profile.png"

    mock_boto3_clients.batch_get_item.return_value = {
        "Responses": {
            "users": [
                {
                    "user_id": {"S": user_id},
                    "login": {"S": expected_login},
                    "profile_image_url": {"S": expected_profile_image_url},
                },
            ],
        },
    }

    users_map = get_users([user_id])

    assert user_id in users_map
    user = users_map[user_id]
    assert user.id == user_id
    assert user.login == expected_login
    assert user.profile_image_url == expected_profile_image_url
    mock_boto3_clients.batch_get_item.assert_called_once_with(
        RequestItems={
            "users": {
                "Keys": [{"user_id": {"S": user_id}}],
            },
        },
    )
    mock_urllib3_pool_manager.request.assert_not_called()


def test_get_users_from_twitch_api_and_cache(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
    mocker,
):
    """
    Test that get_users fetches user data from Twitch API if not in DynamoDB
    and then caches it.
    """
    user_id = "67890"
    expected_login = "newuser"
    expected_profile_image_url = "http://example.com/new_profile.png"

    mock_boto3_clients.batch_get_item.return_value = {"Responses": {"users": []}}  # Not found in DynamoDB

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

        users_map = get_users([user_id])

        assert user_id in users_map
        user = users_map[user_id]
        assert user.id == user_id
        assert user.login == expected_login
        assert user.profile_image_url == expected_profile_image_url

        mock_boto3_clients.batch_get_item.assert_called_once_with(
            RequestItems={
                "users": {
                    "Keys": [{"user_id": {"S": user_id}}],
                },
            },
        )
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.batch_write_item.assert_called_once_with(
            RequestItems={
                "users": [
                    {
                        "PutRequest": {
                            "Item": {
                                "user_id": {"S": user_id},
                                "login": {"S": expected_login},
                                "profile_image_url": {"S": expected_profile_image_url},
                                "expireAt": {"N": str(int(datetime.datetime(2025, 2, 1, 12, 0, 0).timestamp()))},
                            },
                        },
                    },
                ],
            },
        )


def test_get_users_empty_user_ids():
    """
    Test that get_users returns an empty dictionary when an empty list of user IDs is provided.
    """
    assert get_users([]) == {}


def test_get_users_missing_client_id(mocker):
    """
    Test that get_users raises ValueError if TWITCH_CLIENT_ID environment variable is not set.
    """
    mocker.patch("commons.boto3.client")
    mocker.patch.dict("os.environ", {}, clear=True)
    mocker.patch("commons.get_oauth_token", return_value="mock_oauth_token")

    with pytest.raises(ValueError, match="TWITCH_CLIENT_ID environment variable is not set."):
        get_users(["some_user_id"])


def test_get_users_missing_oauth_token(mocker):
    """
    Test that get_users raises ValueError if TWITCH_OAUTH_TOKEN environment variable is not set.
    """
    mocker.patch("commons.boto3.client")
    mocker.patch.dict("os.environ", {}, clear=True)
    mocker.patch("commons.get_client_id", return_value="mock_client_id")

    with pytest.raises(ValueError, match="TWITCH_OAUTH_TOKEN environment variable is not set."):
        get_users(["some_user_id"])


def test_get_users_not_found_in_twitch_api(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
    mocker,
):
    """
    Test that get_users raises ValueError if user not found in Twitch API
    and caches the error.
    """
    user_id = "99999"

    mock_boto3_clients.batch_get_item.return_value = {"Responses": {"users": []}}  # Not found in DynamoDB

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

        users_map = get_users([user_id])

        # Assert that the user is not in the returned map
        assert user_id not in users_map

        mock_boto3_clients.batch_get_item.assert_called_once_with(
            RequestItems={
                "users": {
                    "Keys": [{"user_id": {"S": user_id}}],
                },
            },
        )
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.batch_write_item.assert_called_once_with(
            RequestItems={
                "users": [
                    {
                        "PutRequest": {
                            "Item": {
                                "user_id": {"S": user_id},
                                "error_twitch_api": {"S": "f"},
                                "expireAt": {"N": str(int(datetime.datetime(2025, 2, 1, 12, 0, 0).timestamp()))},
                            },
                        },
                    },
                ],
            },
        )


def test_get_users_dynamodb_cached_error(mock_boto3_clients, mock_urllib3_pool_manager):
    """
    Test that get_users raises ValueError if user is found in DynamoDB
    but marked as an error from a previous Twitch API lookup.
    """
    user_id = "54321"
    mock_boto3_clients.batch_get_item.return_value = {
        "Responses": {
            "users": [
                {
                    "user_id": {"S": user_id},
                    "error_twitch_api": {"S": "f"},
                },
            ],
        },
    }

    users_map = get_users([user_id])

    # Assert that the user is not in the returned map because it's marked as an error
    assert user_id not in users_map

    mock_boto3_clients.batch_get_item.assert_called_once_with(
        RequestItems={
            "users": {
                "Keys": [{"user_id": {"S": user_id}}],
            },
        },
    )
    mock_urllib3_pool_manager.request.assert_not_called()


def test_get_users_twitch_api_http_error(
    mock_boto3_clients,
    mock_urllib3_pool_manager,
):
    """
    Test that get_users handles HTTP errors from Twitch API gracefully.
    """
    user_id = "11223"
    user_id = "11223"  # Redefine user_id for clarity in this test
    mock_boto3_clients.batch_get_item.return_value = {"Responses": {"users": []}}  # Not found in DynamoDB

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
            get_users([user_id])

        mock_boto3_clients.batch_get_item.assert_called_once_with(
            RequestItems={
                "users": {
                    "Keys": [{"user_id": {"S": user_id}}],
                },
            },
        )
        mock_urllib3_pool_manager.request.assert_called_once()
        mock_boto3_clients.batch_write_item.assert_not_called()  # Should not cache on API error


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
