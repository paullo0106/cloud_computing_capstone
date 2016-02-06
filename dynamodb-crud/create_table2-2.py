

import boto3
from my_credential import DYNAMODB_ENDPOINT_URL

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))


table = dynamodb.create_table(
    TableName='Delay2_2',
    KeySchema=[
        {
            'AttributeName': 'origin_airport',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'delay',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'origin_airport',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'delay',
            'AttributeType': 'N'
        }

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)

