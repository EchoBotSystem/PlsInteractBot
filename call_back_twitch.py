import hmac
import json
from pathlib import Path
import aws

aws.init_dynamodb()

# Define constants for Twitch EventSub message header keys.
# These are converted to lowercase as HTTP headers are case-insensitive
# and AWS Lambda's event dictionary typically normalizes them.
MESSAGE_ID_KEY = "Twitch-Eventsub-Message-Id".lower()
MESSAGE_TIMESTAMP_KEY = "Twitch-Eventsub-Message-Timestamp".lower()
MESSAGE_SIGNATURE_KEY = "Twitch-Eventsub-Message-Signature".lower()
MESSAGE_TYPE_KEY = "Twitch-Eventsub-Message-Type".lower()

# Prefix expected in the Twitch-Eventsub-Message-Signature header.
HMAC_PREFIX = "sha256="


def get_secret() -> bytes:
    """
    Reads the HMAC secret from a file named 'secret.txt' located in the same
    directory as this script. This secret is used to validate Twitch signatures.
    """
    path = Path(__file__).parent.absolute()
    with Path.open(path / "secret.txt", "rb") as file:
        return file.read()


def lambda_handler(event: dict, context: dict) -> dict:
    """
    The main entry point for the AWS Lambda function.
    Processes incoming Twitch EventSub notifications.

    Args:
        event (dict): The event dictionary containing request details (headers, body).
        context (dict): The runtime context of the Lambda function.

    Returns:
        dict: A dictionary representing the HTTP response.
    """
    print("Event received", event)

    # Validate essential headers and body presence.
    if not is_valid_event(event):
        print("Invalid event received", event)
        return {"statusCode": 400}  # Bad Request

    # Validate the HMAC signature to ensure the request is from Twitch.
    if not is_valid_signature(event):
        print("Invalid signature for event", event)
        return {"statusCode": 403}  # Forbidden

    # Handle Twitch EventSub challenge requests.
    # Twitch sends these to verify the endpoint during subscription creation.
    if is_challenge(event):
        body = json.loads(event["body"])
        challenge = body.get("challenge")  # Use .get() for safer access
        if challenge:
            print(f"Challenge received: {challenge}")
            # Respond with the challenge string to complete verification.
            return {
                "statusCode": 200,
                "headers": {"content-type": "text/plain"},
                "body": challenge,
            }
        print("No challenge found in the event body")
        return {"statusCode": 400, "body": "No challenge found"}  # Bad Request

    # Handle specific EventSub notifications, e.g., channel chat messages.
    if is_channel_chat_message(event):
        try:
            save_channel_chat_message(event)
        except Exception as e:
            # Log the error but return 200 to prevent Twitch from retrying
            # if the event was successfully processed up to this point.
            print(f"Error saving channel chat message: {e}")
            return {"statusCode": 200}

    # For any other valid event type, return 200 OK.
    return {"statusCode": 200}


def is_valid_event(event: dict) -> bool:
    """
    Checks if the incoming event contains all necessary headers and a body
    expected from a Twitch EventSub notification.
    """
    headers = event.get("headers", {})  # Use .get() for safer access

    # Check for the presence of required Twitch EventSub headers.
    if MESSAGE_ID_KEY not in headers:
        print("Missing message ID in headers")
        return False
    if MESSAGE_TIMESTAMP_KEY not in headers:
        print("Missing message timestamp in headers")
        return False
    if MESSAGE_SIGNATURE_KEY not in headers:
        print("Missing message signature in headers")
        return False
    if MESSAGE_TYPE_KEY not in headers:
        print("Missing message type in headers")
        return False

    # Check for the presence of the request body.
    if "body" not in event:
        print("Missing body in event")
        return False
    return True


def is_valid_signature(event: dict) -> bool:
    """
    Validates the HMAC signature of the incoming event against the shared secret.
    This ensures the request genuinely originated from Twitch.
    """
    headers = event["headers"]
    message_id = headers[MESSAGE_ID_KEY]
    message_timestamp = headers[MESSAGE_TIMESTAMP_KEY]
    message_signature = headers[MESSAGE_SIGNATURE_KEY]
    message_body = event["body"]

    # Construct the message to be signed: id + timestamp + body.
    # This matches Twitch's signature generation method.
    message_to_sign = (message_id + message_timestamp + message_body).encode("utf-8")

    # Generate the HMAC digest using the secret and the message.
    # The result is prefixed with 'sha256=' and converted to hex.
    got_digest = (
        HMAC_PREFIX
        + hmac.digest(
            get_secret(),
            message_to_sign,
            "sha256",
        ).hex()
    )

    print(f"Got digest: {got_digest}")
    print(f"Expected digest: {message_signature}")

    # Use hmac.compare_digest for a constant-time comparison to prevent timing attacks.
    return hmac.compare_digest(got_digest, message_signature)


def is_challenge(event: dict) -> bool:
    """
    Determines if the incoming event is a Twitch EventSub webhook callback verification
    challenge.
    """
    headers = event["headers"]
    return headers.get(MESSAGE_TYPE_KEY) == "webhook_callback_verification"


def is_channel_chat_message(event: dict) -> bool:
    """
    Determines if the incoming event is a Twitch EventSub 'channel.chat.message'
    notification.
    """
    headers = event["headers"]

    # Check message type is 'notification'.
    if headers.get(MESSAGE_TYPE_KEY) != "notification":
        print("Message type is not notification for being an channel chat message")
        return False

    # Parse the body to check subscription type.
    if "body" not in event:
        print("Missing body in event for being an channel chat message")
        return False

    try:
        body = json.loads(event["body"])
    except json.JSONDecodeError:
        print("Could not decode JSON body for channel chat message check")
        return False

    # Navigate through the JSON structure to find the subscription type.
    subscription_type = body.get("subscription", {}).get("type")
    if subscription_type != "channel.chat.message":
        print(
            "Subscription type is not channel.chat.message for being an channel chat message",
        )
        return False
    return True


def save_channel_chat_message(event: dict) -> None:
    """
    Extracts relevant information from a 'channel.chat.message' event
    and saves it to a DynamoDB table named 'comments'.
    """
    body = json.loads(event["body"])

    # Extract data points from the event body.
    message_id = body["event"]["message_id"]
    chatter_user_id = body["event"]["chatter_user_id"]
    broadcaster_user_id = body["subscription"]["condition"]["broadcaster_user_id"]
    message_content = body["event"]["message"]["text"]

    # Get the reception time from the Lambda request context.
    # This is typically a Unix epoch timestamp.
    reception_unixtime = str(event["requestContext"]["timeEpoch"])

    # Put the item into the 'comments' DynamoDB table.
    # Each attribute needs to specify its type (e.g., "S" for String, "N" for Number).
    aws.dynamodb.put_item(
        TableName="comments",
        Item={
            "message_id": {"S": message_id},
            "chatter_user_id": {"S": chatter_user_id},
            "broadcaster_user_id": {"S": broadcaster_user_id},
            "message_content": {"S": message_content},
            "reception_unixtime": {"N": reception_unixtime},
        },
    )
    print(
        f"Saved channel chat message: {message_id} from {chatter_user_id} to {broadcaster_user_id}"
    )
