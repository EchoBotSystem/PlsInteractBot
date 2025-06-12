import time
from collections import Counter
import boto3
import dataclasses
import urllib3
import os
import datetime
import json


def get_ranking(
    start_unixtime: int = int(time.time() * 1000)
    - (60 * 60 * 24 * 30 * 1000),  # Now minus 30 days in milliseconds
    end_unixtime: int = int(time.time() * 1000),  # Now in milliseconds
) -> list[
    dict[
        str,  # user_id
        int,  # message_count
    ]
]:
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

    top_chatters = chatter_message_counts.most_common(10)
    ranking = []
    for user_id, count in top_chatters:
        try:
            user_data = get_user(user_id)
        except ValueError as e:
            print(f"Error fetching user {user_id}: {e}")
            continue
        ranking.append(
            {
                "user_id": user_data.id,
                "user_login": user_data.login,
                "message_count": count,
                "profile_image_url": user_data.profile_image_url,
            }
        )
    return ranking


@dataclasses.dataclass(
    frozen=True,
    slots=True,
)
class User:
    id: str
    login: str
    profile_image_url: str


def get_user(user_id: str) -> User:
    dynamodb = boto3.client("dynamodb")
    user_data = dynamodb.get_item(
        TableName="users",
        Key={"user_id": {"S": user_id}},
    )

    if "Item" in user_data:
        item = user_data["Item"]
        if "error_twitch_api" in item and item["error_twitch_api"]["S"] == "f":
            print(f"User {user_id} found in DynamoDB but marked as error")
            raise ValueError(f"User {user_id} not found in Twitch API")
        return User(
            id=user_id,
            login=item["login"]["S"],
            profile_image_url=item["profile_image_url"]["S"],
        )

    print(f"User {user_id} not found in DynamoDB, fetching from Twitch API")
    twitch_user_http_response = urllib3.PoolManager().request(
        "GET",
        "https://api.twitch.tv/helix/users",
        fields={"id": user_id},
        headers={
            "Client-ID": get_client_id(),
            "Authorization": f"Bearer {get_oauth_token()}",
        },
    )
    twitch_user = json.loads(twitch_user_http_response.data.decode("utf-8"))
    if len(twitch_user["data"]) == 0:
        dynamodb.put_item(
            TableName="users",
            Item={
                "user_id": {"S": user_id},
                "error_twitch_api": {"S": "f"},
                "expireAt": {
                "N": str(
                    int(
                        (
                            datetime.datetime.now() + datetime.timedelta(days=1)
                        ).timestamp()
                    )
                )
            },
            },
        )
        raise ValueError(f"User {user_id} not found in Twitch API")

    dynamodb.put_item(
        TableName="users",
        Item={
            "user_id": {"S": user_id},
            "login": {"S": twitch_user["data"][0]["login"]},
            "profile_image_url": {"S": twitch_user["data"][0]["profile_image_url"]},
            "expireAt": {
                "N": str(
                    int(
                        (
                            datetime.datetime.now() + datetime.timedelta(days=1)
                        ).timestamp()
                    )
                )
            },
        },
    )
    return User(
        id=twitch_user["data"][0]["id"],
        login=twitch_user["data"][0]["login"],
        profile_image_url=twitch_user["data"][0]["profile_image_url"],
    )


def get_client_id() -> str:
    return os.environ.get("TWITCH_CLIENT_ID")


def get_oauth_token() -> str:
    return os.environ.get("TWITCH_OAUTH_TOKEN")
