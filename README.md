# PlsInteractBot

## About the Project

PlsInteractBot is a self-contained SaaS product designed to enhance interaction on Twitch channels. It integrates directly with the Twitch platform to monitor chat activity, store messages, and generate real-time rankings of the most active chatters. This bot operates independently, leveraging AWS Lambda for serverless execution and Amazon DynamoDB for robust data storage.

PlsInteractBot functions as a third-party application that seamlessly interacts with Twitch channels. It's built as a self-contained SaaS solution, meaning it handles its own operations and data management. While independent, it relies heavily on the Twitch API for real-time chat access and Amazon DynamoDB for persistent storage of chat data and user rankings.

The core functionalities of PlsInteractBot include:

*   **Twitch Integration:** Connects to specified Twitch channels to receive live chat messages via Twitch EventSub.
*   **Chat Message Ingestion:** Reads and processes incoming chat messages from connected channels.
*   **Data Storage:** Stores raw chat messages and associated user information (like `chatter_user_id`, `broadcaster_user_id`, `message_content`, `reception_unixtime`) in a DynamoDB table named `comments`.
*   **Activity Aggregation:** Counts the number of comments posted by each unique user within a defined time window (currently 30 days).
*   **User Ranking Generation:** Generates a ranking of users based on their comment count, identifying the top chatters. This data is stored in a DynamoDB table named `rankings`.
*   **Ranking Interface (TBD):** While not yet implemented, the project envisions providing an interface (likely web-based) to view the generated user rankings.

## Getting Started

### Prerequisites
*   AWS Account
*   Twitch Developer Account
*   Python 3.12

## LICENSE

[MIT License](LICENSE)