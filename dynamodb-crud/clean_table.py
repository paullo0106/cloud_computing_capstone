

import sys
import boto3
from my_credential import DYNAMODB_ENDPOINT_URL


dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))


if __name__ == '__main__':

	table_name = sys.argv[1]

	print 'Delete table {}'.format(table_name)
	table = dynamodb.Table(table_name)
	table.delete()


