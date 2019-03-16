import boto3
import os
import pprint
import time
from slacker import Slacker

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

sts_client = boto3.client('sts')
assumed_role_object = sts_client.assume_role(
   RoleArn="ops_account_admin_role_arn",
   RoleSessionName="AssumedRoleSession01"
)
credentials = assumed_role_object['Credentials']
AWS_ACCESS_KEY_IDS = credentials['AccessKeyId']
AWS_SECRET_ACCESS_KEYS = credentials['SecretAccessKey']
AWS_SESSION_TOKENS = credentials['SessionToken']

ssmsession = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                           region_name='us-west-2')
ssm = ssmsession.client('ssm')
SLACK_SSM_PASSWORD = '/prod/us-west-2/SRE/slack/justinpass'
slack_password = ssm.get_parameter(Name=SLACK_SSM_PASSWORD, WithDecryption=True)


def rundmsjobs():
    dms_jobs = (
        "arn:aws:dms:us-west-2:256095309945:task:5Y2KZT7RRRILTWH3QGK72VGCBE",
        "arn:aws:dms:us-west-2:256095309945:task:H6VUONQGX36UI3NXQLKQI4KPGA",
        "arn:aws:dms:us-west-2:256095309945:task:PD4C4JTOKKKF36BRVAFB7K4DKI",
        "arn:aws:dms:us-west-2:256095309945:task:QSOVFUZZ5ZK7F7PKRN55S43AHE",
        "arn:aws:dms:us-west-2:256095309945:task:TU2SFHG4PINXJPVZSJ4IC47Y2U",
        "arn:aws:dms:us-west-2:256095309945:task:XJLQ57G6BIYYOTWTABUTM3YPYQ"
    )
    dmssession = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name='us-west-2')
    dms = dmssession.client('dms')
    for job in dms_jobs:
        response = dms.start_replication_task(
            ReplicationTaskArn='%s' % job,
            StartReplicationTaskType='reload-target')
        #  pprint.pprint("DMS response: %s" % response)
        sendtoslack(response)
        time.sleep(300)


def sendtoslack(dms_response):
    slack = Slacker('{}'.format(slack_password['Parameter']['Value']))
    slack.chat.post_message(channel='sys-monitor-logs',
                            text=dms_response,
                            username='MongoBot'
                            )


rundmsjobs()
