import dataclasses
import datetime
import json
import os
import time
from collections import Counter

import boto3
import urllib3

THIRTY_DAYS_IN_MS = 60 * 60 * 24 * 30 * 1000


@dataclasses.dataclass(
    frozen=True,
    slots=True,
)
class User:
    id: str
    login: str
    profile_image_url: str


def get_ranking(
    start_unixtime: int | None = None,
    end_unixtime: int | None = None,
) -> list[
    dict[
        str,  # user_id
        int,  # message_count
    ]
]:
    """
    Retrieves and aggregates chat message counts for users within a specified time range
    from DynamoDB, then fetches user details from Twitch API for the top chatters.

    Args:
        start_unixtime (int | None): The start of the time range in Unix milliseconds.
                                     Defaults to 30 days ago if None.
        end_unixtime (int | None): The end of the time range in Unix milliseconds.
                                   Defaults to the current time if None.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents a top chatter
                    and contains their user ID, login, message count, and profile image URL.
                    Example:
                    [
                        {
                            "userId": "12345",
                            "userLogin": "streamer1",
                            "messageCount": 150,
                            "profileImageUrl": "http://example.com/streamer1.png",
                        },
                        ...
                    ]
    """
    current_time_ms = int(time.time() * 1000)
    if end_unixtime is None:
        end_unixtime = current_time_ms
    if start_unixtime is None:
        start_unixtime = current_time_ms - THIRTY_DAYS_IN_MS

    print(f"Getting ranking messages from Unix time {start_unixtime} to {end_unixtime}")

    dynamodb = boto3.client("dynamodb")
    all_messages = []
    last_evaluated_key = None

    while True:
        scan_params = {
            "TableName": "comments",
            "ProjectionExpression": "chatter_user_id, reception_unixtime",
            "FilterExpression": "reception_unixtime BETWEEN :start_time AND :end_time",
            "ExpressionAttributeValues": {
                ":start_time": {"N": str(start_unixtime)},
                ":end_time": {"N": str(end_unixtime)},
            },
        }
        if last_evaluated_key:
            scan_params["ExclusiveStartKey"] = last_evaluated_key
        response = dynamodb.scan(**scan_params)
        all_messages.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    chatter_message_counts = Counter()
    for item in all_messages:
        chatter_user_id = item.get("chatter_user_id", {}).get("S")
        if chatter_user_id:
            chatter_message_counts[chatter_user_id] += 1

    print(f"Aggregated chatter message counts: {chatter_message_counts}")

    top_chatters_ids = [user_id for user_id, _ in chatter_message_counts.most_common(10)]

    user_data_map = {}
    if top_chatters_ids:
        # Fetch user data for all top chatters in a single batched operation
        # get_users will handle caching and Twitch API calls efficiently
        user_data_map = get_users(top_chatters_ids)

    ranking = []
    for user_id, count in chatter_message_counts.most_common(10):
        user_data = user_data_map.get(user_id)
        if user_data:  # Only include users for whom data was successfully retrieved
            ranking.append(
                {
                    "userId": user_data.id,
                    "userLogin": user_data.login,
                    "messageCount": count,
                    "profileImageUrl": user_data.profile_image_url,
                },
            )
        else:
            print(f"Skipping user {user_id} as data could not be retrieved.")
    return ranking


def get_users(user_ids: list[str]) -> dict[str, User]:
    """
    Retrieves user information (login, profile image URL) for a list of user IDs.
    It first attempts to fetch users from a DynamoDB cache using batch_get_item.
    For users not found in cache or marked as an error, it fetches them from the
    Twitch API using a single batched request. Successful lookups from Twitch API
    are cached in DynamoDB with an expiration time using batch_write_item.

    Args:
        user_ids (list[str]): A list of Twitch user IDs.

    Returns:
        dict[str, User]: A dictionary where keys are user IDs and values are User
                         dataclass instances for all successfully retrieved users.
                         Users not found in Twitch API will not be present in this dictionary.

    Raises:
        urllib3.exceptions.MaxRetryError: If there's an HTTP error when calling the Twitch API.
    """
    if not user_ids:
        return {}

    dynamodb = boto3.client("dynamodb")
    found_users_map: dict[str, User] = {}
    twitch_fetch_ids: list[str] = []
    cache_write_requests: list[dict] = []

    # 1. Check DynamoDB cache using batch_get_item
    request_items = {
        "users": {
            "Keys": [{"user_id": {"S": user_id}} for user_id in user_ids],
        },
    }
    response = dynamodb.batch_get_item(RequestItems=request_items)

    for item in response.get("Responses", {}).get("users", []):
        user_id = item["user_id"]["S"]
        if "error_twitch_api" in item and item["error_twitch_api"]["S"] == "f":
            print(f"User {user_id} found in DynamoDB but marked as error (skipping)")
            # Do not add to found_users_map, will not be fetched from Twitch again for now
        else:
            found_users_map[user_id] = User(
                id=user_id,
                login=item["login"]["S"],
                profile_image_url=item["profile_image_url"]["S"],
            )

    # Let's adjust the logic to correctly identify twitch_fetch_ids
    cached_user_ids = set(found_users_map.keys())
    # Add user_ids that were found as errors in cache to cached_user_ids so they are not re-fetched
    for item in response.get("Responses", {}).get("users", []):
        user_id = item["user_id"]["S"]
        if "error_twitch_api" in item and item["error_twitch_api"]["S"] == "f":
            cached_user_ids.add(user_id)  # Mark error-cached IDs as "processed"

    for user_id in user_ids:
        if user_id not in cached_user_ids:
            twitch_fetch_ids.append(user_id)

    if not twitch_fetch_ids:
        return found_users_map

    print(f"Users {twitch_fetch_ids} not found in DynamoDB, fetching from Twitch API")

    # 2. Fetch from Twitch API in a single batched request
    # Twitch API expects multiple 'id' query parameters for batching
    fields = [("id", user_id) for user_id in twitch_fetch_ids]

    client_id = get_client_id()
    oauth_token = get_oauth_token()

    if not client_id:
        raise ValueError("TWITCH_CLIENT_ID environment variable is not set.")
    if not oauth_token:
        raise ValueError("TWITCH_OAUTH_TOKEN environment variable is not set.")

    http = urllib3.PoolManager()
    twitch_user_http_response = http.request(
        "GET",
        "https://api.twitch.tv/helix/users",
        fields=fields,
        headers={
            "Client-ID": client_id,
            "Authorization": f"Bearer {oauth_token}",
        },
    )
    twitch_response_data = json.loads(twitch_user_http_response.data.decode("utf-8"))

    # Prepare expiration time for cache entries (1 day from now)
    expire_at_timestamp = str(int((datetime.datetime.now() + datetime.timedelta(days=1)).timestamp()))

    # Process Twitch API response
    fetched_twitch_ids = set()
    for user_data in twitch_response_data.get("data", []):
        user_id = user_data["id"]
        fetched_twitch_ids.add(user_id)
        user = User(
            id=user_data["id"],
            login=user_data["login"],
            profile_image_url=user_data["profile_image_url"],
        )
        found_users_map[user_id] = user
        cache_write_requests.append(
            {
                "PutRequest": {
                    "Item": {
                        "user_id": {"S": user_id},
                        "login": {"S": user.login},
                        "profile_image_url": {"S": user.profile_image_url},
                        "expireAt": {"N": expire_at_timestamp},
                    },
                },
            },
        )

    # Mark users not found in Twitch API as errors in cache
    for user_id in twitch_fetch_ids:
        if user_id not in fetched_twitch_ids:
            print(f"User {user_id} not found in Twitch API (caching error)")
            cache_write_requests.append(
                {
                    "PutRequest": {
                        "Item": {
                            "user_id": {"S": user_id},
                            "error_twitch_api": {"S": "f"},
                            "expireAt": {"N": expire_at_timestamp},
                        },
                    },
                },
            )

    # 3. Batch write new cache entries to DynamoDB
    if cache_write_requests:
        # batch_write_item has a limit of 25 items per request.
        # For the current use case (top 10 chatters), this limit is unlikely to be hit.
        # For larger batches, this would need to be chunked into multiple calls.
        dynamodb.batch_write_item(RequestItems={"users": cache_write_requests})

    return found_users_map


def get_client_id() -> str | None:
    """
    Retrieves the Twitch client ID from environment variables.

    Returns:
        str | None: The Twitch client ID if set, otherwise None.
    """
    return os.environ.get("TWITCH_CLIENT_ID")


def get_oauth_token() -> str | None:
    """
    Retrieves the Twitch OAuth token from environment variables.

    Returns:
        str | None: The Twitch OAuth token if set, otherwise None.
    """
    return os.environ.get("TWITCH_OAUTH_TOKEN")
