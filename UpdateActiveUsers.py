import psycopg2
import boto3
import os
from slacker import Slacker

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
REDSHIFT_SSM_USER = '/prod/us-west-2/SRE/redshift/bi_user'
REDSHIFT_SSM_PASSWORD = '/prod/us-west-2/SRE/redshift/bi_pass'
SLACK_SSM_PASSWORD = '/prod/us-west-2/SRE/slack/pass'
session = boto3.Session(region_name='us-west-2')
ssm = session.client('ssm')
redshift_user = ssm.get_parameter(Name=REDSHIFT_SSM_USER, WithDecryption=True)
redshift_password = ssm.get_parameter(Name=REDSHIFT_SSM_PASSWORD, WithDecryption=True)
slack_password = ssm.get_parameter(Name=SLACK_SSM_PASSWORD, WithDecryption=True)

conn = psycopg2.connect(dbname='dwprod', user=redshift_user['Parameter']['Value'], host='dwprod.subsplash.net', password=redshift_password['Parameter']['Value'], port=5439)
cur = conn.cursor()


def sendtoslack(message):
    slack = Slacker('{}'.format(slack_password['Parameter']['Value']))
    slack.chat.post_message(channel='my-testing-channel',
                            text='%s' % (message),
                            username='ActiveUsersBot'
                            )


sendtoslack("Beginning update of active users tables")
print("Beginning update of active users tables")
sendtoslack("Dropping table public.app_launches_au_day")
print("Dropping table public.app_launches_au_day")
cur.execute("""DROP TABLE public.app_launches_au_day;""")
sendtoslack("Creating table public.app_launches_au_day")
print("Creating table public.app_launches_au_day")
cur.execute(""" CREATE TABLE public.app_launches_au_day AS
                SELECT
                       date_trunc('day', created_at) AS day,
                       COUNT (DISTINCT suiid) AS suiid_count,
                       COUNT (DISTINCT uid) AS uid_count
                FROM
                       app_launches_active_user
                WHERE
                       created_at::DATE >= '2014-01-01'
                GROUP BY
                       day;""")
sendtoslack("Dropping table public.app_launches_au_week")
print("Dropping table public.app_launches_au_week")
cur.execute("""DROP TABLE public.app_launches_au_week;""")
sendtoslack("Creating table public.app_launches_au_week")
print("Creating table public.app_launches_au_week")
cur.execute(""" CREATE TABLE public.app_launches_au_week AS
                SELECT
                       date_trunc('week', created_at) AS week,
                       COUNT (DISTINCT suiid) AS suiid_count,
                       COUNT (DISTINCT uid) AS uid_count
                FROM
                       app_launches_active_user
                WHERE
                       created_at::DATE >= '2014-01-01'
                GROUP BY
                       week;""")
sendtoslack("Dropping table public.app_launches_au_month")
print("Dropping table public.app_launches_au_month")
cur.execute("""DROP TABLE public.app_launches_au_month;""")
sendtoslack("Creating table public.app_launches_au_month")
print("Creating table public.app_launches_au_month")
cur.execute(""" CREATE TABLE public.app_launches_au_month AS
                SELECT
                       date_trunc('month', created_at) AS month,
                       COUNT (DISTINCT suiid) AS suiid_count,
                       COUNT (DISTINCT uid) AS uid_count
                FROM
                       app_launches_active_user
                WHERE
                       created_at::DATE >= '2014-01-01'
                GROUP BY
                       month;""")
sendtoslack("Updating active users tables complete.")
print("Updating active users tables complete.")
