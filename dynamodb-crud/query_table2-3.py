
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from my_credential import DYNAMODB_ENDPOINT_URL

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

table_name = 'Delay2_3'
table = dynamodb.Table(table_name)

if __name__ == '__main__':

	o = sys.argv[1]
	d = sys.argv[2]

	response = table.query(
    		KeyConditionExpression=Key('origin_airport').eq(o)
	)

	print 'From {} to {}:'.format(o, d)
	for i in response['Items']:
        	if i['dest_airport'] == d:
                	print '{} delays {} on average'.format(i['carrier'], round(i['delay'],2))
