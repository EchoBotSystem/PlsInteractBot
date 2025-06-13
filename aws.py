import os

import boto3

dynamodb = None
dynamodb_resource = None
connections_table = None
comments_table = None
users_table = None

apigw = None

def init_dynamodb() -> None:
    global dynamodb, dynamodb_resource
    if dynamodb is None:
        dynamodb = boto3.client("dynamodb")
    if dynamodb_resource is None:
        dynamodb_resource = boto3.resource("dynamodb")

def init_connections_table() -> None:
    global connections_table
    if connections_table is None:
        connections_table = dynamodb_resource.Table(os.environ["CONNECTIONS_TABLE_NAME"])

def init_comments_table() -> None:
    global comments_table
    if comments_table is None:
        comments_table = dynamodb_resource.Table(os.environ["COMMENTS_TABLE_NAME"])

def init_users_table() -> None:
    global users_table
    if users_table is None:
        users_table = dynamodb_resource.Table(os.environ["USERS_TABLE_NAME"])

def init_apigw() -> None:
    global apigw
    if apigw is None:
        domain = os.environ["DOMAIN"]
        stage = os.environ["STAGE"]
        apigw = boto3.client("apigatewaymanagementapi", endpoint_url=f"https://{domain}/{stage}")