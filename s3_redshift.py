import json # for JSON data
import csv
import boto3 # used to interact AWS services like we interact with S3 here
import psycopg2 # PostgreSQL adapter that is used to interact with the Redshift database.
import redshift_connector

s3 = boto3.client('s3') # s3 is S3 client object
print("Start")
redshift = redshift_connector.connect(
    host='my-workgroup.442597812037.us-east-1.redshift-serverless.amazonaws.com',
    database='dev',
    user='ssr',
    password='Ssr12345',
    port=5439
)
print("conn - Done")

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        rows = file_content.split('\n')
        cursor = redshift.cursor()
        for row in csv.reader(rows):
            cursor.execute("COPY redshiftaman FROM 's3://mybucket-aman/Demo.csv' STDIN CREDENTIALS 'aws_access_key_id=AKIAWODHCU5CUCUKSTBX;aws_secret_access_key=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' DELIMITER ',' IGNOREHEADER 1 CSV;")
            # cursor.execute("INSERT INTO redshiftaman VALUES (%s, %s, %s, %s)", row)
            #copy_command = "COPY redshiftaman FROM 's3://mybucket-aman/Demo.csv' CSV;"
            #cursor.execute(copy_command)
        redshift.commit()
    return {
        'statusCode': 200,
        'body': json.dumps('Data loaded into Redshift')
    }

event = {
    'Records': [
        {
            's3': {
                'bucket': {
                    'name': 'mybucket-aman'
                },
                'object': {
                    'key': 'Demo.csv'
                }
            }
        }
    ]
}

context = {}
print(lambda_handler(event, context))

# export AWS_ACCESS_KEY_ID='AKIAWODHCU5CUCUKSTBX'
# export AWS_SECRET_ACCESS_KEY='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'



