from pymongo import MongoClient
import urllib.parse
import json
import datetime
import boto3
import os
from slacker import Slacker

'''
This script assumes the following:
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables are set.
Access to AWS SSM parameter store for mongodb admin password.

The script is self contained and does not take any parameters.
'''

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
now = datetime.datetime.now()
filename = "mongodb" + str(now.strftime("%Y%m%d_%H.%M.%S")) + "_PT" + ".json"
MONGO_SSM_PASSWORD = '/prod/us-west-2/SRE/mongodb/admin_pass'
SLACK_SSM_PASSWORD = '/prod/us-west-2/SRE/slack/pass'
session = boto3.Session(region_name='us-west-2')
ssm = session.client('ssm')
mongo_password = ssm.get_parameter(Name=MONGO_SSM_PASSWORD, WithDecryption=True)
slack_password = ssm.get_parameter(Name=SLACK_SSM_PASSWORD, WithDecryption=True)

servers = ("mongodb-analytics-00.awsusw2.domain.net",
           "mongodb-analytics-01.awsusw2.domain.net",
           "mongodb-analytics-02.awsusw2.domain.net",
           "mongodb-analytics-03.awsusw2.domain.net",
           "mongodb-analytics-04.awsusw2.domain.net",
           "mongodb-analytics-05.awsusw2.domain.net")


def lambda_handler(event, context):
    # TODO implement as lambda
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def getfromshard(usersname, passw, serversname):

    client = MongoClient("mongodb://%s:%s@%s:27000/?authsource=admin" % (usersname, passw, serversname))

    try:
        with open(filename, 'a') as outfile:
            for database in client.list_database_names():
                for collection in client[database].list_collection_names():
                    if collection == "system.profile":
                        for document in client[database][collection].find({}).sort([{'millis', -1}]).limit(5):
                            # print(json.dumps(document, default=dateconv, indent=4))
                            outfile.write(json.dumps(document, default=dateconv, indent=4))

    finally:
        client.close()


def dateconv(datetoconv):
    if isinstance(datetoconv, datetime.datetime):
        return datetoconv.__str__()


def sendtos3():
    data = open(filename, 'rb')
    s3 = boto3.resource('s3')
    object = s3.Object('01-logs', 'slow_queries/%s' % filename)
    object.put(Body=data, ACL='bucket-owner-full-control')


def sendtoslack():
    slack = Slacker('{}'.format(slack_password['Parameter']['Value']))
    slack.chat.post_message(channel='sys-monitor-logs',
                            text='Mongo slow query file posted to %s%s. You should see it there in a couple of minutes' % ('https://slowreports.docs.domain.net/', filename),
                            username='MongoBot'
                            )


for server in servers:
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('{}'.format(mongo_password['Parameter']['Value']))
    servername = urllib.parse.quote_plus('%s' % server)
    getfromshard(username, password, servername)

sendtos3()
sendtoslack()
