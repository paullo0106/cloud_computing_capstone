

import boto3
from my_credential import DYNAMODB_ENDPOINT_URL

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

table = dynamodb.create_table(
    TableName='Group3_2',
    KeySchema=[
        {
            'AttributeName': 'route',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'depart_date',
            'KeyType': 'RANGE'  #Partition key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'route',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'depart_date',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)

