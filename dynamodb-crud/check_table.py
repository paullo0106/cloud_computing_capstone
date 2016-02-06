
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from my_credential import DYNAMODB_ENDPOINT_URL


dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

if __name__ == '__main__':

	table_name = sys.argv[1]

	print table_name
	table = dynamodb.Table(table_name)
	
	all = table.scan()
	print '{} record in {} table'.format(all.get('Count', 0), table_name)

