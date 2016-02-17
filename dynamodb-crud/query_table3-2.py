
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from my_credential import DYNAMODB_ENDPOINT_URL

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

table_name = 'Group3_2'
table = dynamodb.Table(table_name)

if __name__ == '__main__':

        if len(sys.argv) < 2:
                response = table.scan()
                for i in response['Items']:
                        print i['route'], i['depart_date']

                exit()

        airport1 = sys.argv[1]
        airport2 = sys.argv[2]
        airport3 = sys.argv[3]
        depart_date = sys.argv[4]

        route = '{} {} {}'.format(airport1, airport2, airport3)

	response = table.query(
    	KeyConditionExpression=Key('route').eq(route)
	)

    	print 'Route {}->{}->{} on {}:'.format(airport1, airport2, airport3, depart_date)
    	for i in response['Items']:
            #print i['origin_airport'], i['transition_airport'], i['dest_airport'], str(i['depart_date'])
            #if i['origin_airport'] == airport1 and i['dest_airport'] == airport3 and str(i['depart_date']) == depart_date:
            if str(i['depart_date']) == depart_date:
                        print '{}->{} (by {}) then {}->{} (by {}), historical delay: {}'.format(
                                        airport1, airport2, i['origin_carrier'],
                                        airport2, airport3, i['dest_carrier'], i['delay'])
