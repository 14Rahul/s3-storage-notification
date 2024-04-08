import boto3
import requests
import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')
SLACK_NOTIFICATION = "https://hooks.slack.com/services/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"
def send_slack_notification(message):
    header = {"Content-type": "application/json"}
    content = {"text": message}
    slack_webhook = (SLACK_NOTIFICATION)
    resp = requests.post(headers=header, json=content, url=slack_webhook)
    if not resp.ok:
        print("Failed Sending message!")
def lambda_handler(event, context):
    bucket_name = 'prodigaltech-db-backup'
    prefix = 'prod/daily/'
    backup_found = False
    # Get today's and yesterday's dates in the format: YYYYMMDD
    day_before = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%d/%m/%Y')
    today_date = datetime.datetime.now().strftime('%Y%m%d')
    day_before_yday = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')
    result = s3.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    for obj in result.get('CommonPrefixes'):
        folder_date = obj.get('Prefix').split('/')[2].split('.')[1].split('_')[0]

        if folder_date == day_before_yday:
            backup_found = True
            break
    if not backup_found:
        message = f"US DB Backups are lagging!! Backup is missing after {day_before}"
        print(message)
        send_slack_notification(message)
