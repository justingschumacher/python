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
filename = str(now.strftime("%Y%m%d_%H.%M.%S")) + "_PT" + ".json"
MONGO_SSM_PASSWORD = '/prod/us-west-2/SRE/mongodb/admin_pass'
SLACK_SSM_PASSWORD = '/prod/us-west-2/SRE/slack/pass'
session = boto3.Session(region_name='us-west-2')
ssm = session.client('ssm')
mongo_password = ssm.get_parameter(Name=MONGO_SSM_PASSWORD, WithDecryption=True)
slack_password = ssm.get_parameter(Name=SLACK_SSM_PASSWORD, WithDecryption=True)

servers = ("mongodb-analytics-00.awsusw2.subsplash.net",
           "mongodb-analytics-01.awsusw2.subsplash.net",
           "mongodb-analytics-02.awsusw2.subsplash.net",
           "mongodb-analytics-03.awsusw2.subsplash.net",
           "mongodb-analytics-04.awsusw2.subsplash.net",
           "mongodb-analytics-05.awsusw2.subsplash.net")


def lambda_handler(event, context):
    # TODO implement as lambda
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def getfromshard(usersname, passw, serversname):
    databases = ("impressions",
                 "push",
                 "app_launches",
                 "app_metrics",
                 "appuser_auth",
                 "appuser_content",
                 "appuser_logins",
                 "file_downloads",
                 "notes",
                 "push",
                 "usercontent"
                 )
    client = MongoClient("mongodb://%s:%s@%s:27000/?authsource=admin" % (usersname, passw, serversname))
    for database in databases:
        database = client[database]
        collection = database["system.profile"]
        cursor = collection.find({})
        #  Mapping example below
    #     # cursor = collection.find({},
    #     #                          {"allUsers": {"$slice": 2},
    #     #                           "appName": 1,
    #     #                           "client": 1,
    #     #                           "command": {"$slice": 2},
    #     #                           "locks": {"$slice": 3},
    #     #                           "millis": 1,
    #     #                           "ns": 1,
    #     #                           "numYield": 1,
    #     #                           "nreturned": 1,
    #     #                           "planSummary": 1,
    #     #                           "op": 1,
    #     #                           "protocol": 1,
    #     #                           "responseLength": 1,
    #     #                           "user": 1,
    #     #                           "query": 7,
    #     #                           "keysExamined": 1,
    #     #                           "docsExamined": 1,
    #     #                           "hasSortStage": 1,
    #     #                           "cursorExhausted": 1,
    #     #                           "ts": 1,
    #     #                           "execStats": 12
    #     #                           }).limit(20)

    try:
        with open(filename, 'w') as outfile:
            for document in cursor:
                print(json.dumps(document, default=dateconv, indent=4))  # debugging
                outfile.write(json.dumps(document, default=dateconv, indent=4))
        # outfile.close()  # potentially unneeded.

    finally:
        cursor.close()
        client.close()


def dateconv(datetoconv):
    if isinstance(datetoconv, datetime.datetime):
        return datetoconv.__str__()


def sendtos3():
    data = open(filename, 'rb')
    s3 = boto3.resource('s3')
    object = s3.Object('01-logs', 'mongolongqueries/%s' % filename)
    object.put(Body=data, ACL='bucket-owner-full-control')


def sendtoslack():
    slack = Slacker('{}'.format(slack_password['Parameter']['Value']))
    slack.chat.post_message(channel='sys-monitor-logs',
                            text='Mongo slow query file posted to %s%s' % ('s3://01-logs/mongolongqueries/', filename),
                            username='MongoBot'
                            )


for server in servers:
    username = urllib.parse.quote_plus('admin')
    password = urllib.parse.quote_plus('{}'.format(mongo_password['Parameter']['Value']))
    servername = urllib.parse.quote_plus('%s' % server)
    getfromshard(username, password, servername)

sendtos3()
sendtoslack()
