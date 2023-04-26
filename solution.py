import os
import boto3
import datetime
import pandas as pd
from io import BytesIO
from io import StringIO
import json
import csv
import numpy as np

import utils
import traceback
import helper

DB_NAME = "dev"

STATUS_INSERT_INPROGRESS = "INSERT_INPROGRESS"
INSERT_COMPLETED = "INSERT_COMPLETED"
STATUS_NOT_FOUND = "FILE_NOT_FOUND"
STATUS_INVALID = "FILE_INVALID"

BING_BUCKET_NAME = 'search-as-service-prod'

ALL_BING_SRC_PREFIX = [
    'bing/crawl_email_priority_output/',
    'bing/priority_output/',
    'bing/crawl_email_output/',
    'bing/output/'
]

BING_OUTPUT_COMPANY_DOMAIN = 'bing/crawl_email_output/ge_bing_company_to_domain/'
BING_OUTPUT_COMPANY_EMAIL = 'bing/crawl_email_output/ge_bing_company_email/'

s3_client = boto3.client('s3')
redshift_client = boto3.client('redshift-data')

def save_to_s3_company_domain(bucket, key, data):
    s3_client.put_object(Bucket=bucket, Body=data.getvalue(), Key=key)
    print("File successfully uploaded in S3!!!")
    return ""

def save_to_s3_company_email(bucket, key, data):
    s3_client.put_object(Bucket=bucket, Body=data.getvalue(), Key=key)
    print("File successfully uploaded in S3!!!")
    return ""

def verify_column_bing(df):
    columns = ['Keyword', 'Organic_Results']
    return all(item.lower() in df.columns.str.lower() for item in columns)
    
def create_schema(table):
    
    table_schema = """
        algo_service_bing_company_to_domain (
          search character varying(400),
      	  created_at character varying(400),
          position character varying(400),
          display_url character varying(400),
          name character varying(1024),
          snippet character varying(6500),
          domain character varying(408),
          search_engine character varying(200),
          external_id character varying(65535),
          rich_snippets character varying(65535),
          country_code character varying(20),
          creation_date character varying(50)
    );"""
    if table == "algo_service_company_email":
        table_schema = """
            algo_service_company_email (
              company_name character varying(400),
          	  email character varying(400)
        );"""
    
    create_sql = f"""
    
        DROP TABLE {table};
        
        CREATE TABLE {table_schema};
    
    """
    print("sql: \n", create_sql)
    
    query_resp = utils.execute_query(client=redshift_client, db_name=DB_NAME, sql=create_sql)

    print("query_resp:", query_resp)

    
def move_s3_to_redshift(bucket, key, table):
    print("-"*50 + "Copy to redshift:" + "-"*50)
    copy_sql = f"""
    
        COPY {table}
            from
            's3://{bucket}/{key}'
            iam_role 'arn:aws:iam::377041650971:role/RedshiftLoaderRole' 
            region 'eu-west-1'  csv  acceptinvchars AS '-' IGNOREHEADER 1 
            truncatecolumns  MAXERROR 10 timeformat 'YYYY-MM-DDTHH:MI:SS';
    """
    print("sql: \n", copy_sql)
    
    query_resp = utils.execute_query(client=redshift_client, db_name=DB_NAME, sql=copy_sql)

    print("query_resp:", query_resp)
    
def get_company_to_domain():
    sql = f"""select count(1) from algo_service_bing_company_to_domain;"""
    sql_status = utils.execute_query(client=redshift_client, db_name=DB_NAME, sql=sql)
    id = sql_status['response']['Id']
    sql_result = utils.get_query_result(redshift_client, id)
    count_company_to_domain = sql_result["Records"][0][0]['longValue']
    print('count_company_to_domain: ', count_company_to_domain)
    return count_company_to_domain
    
def get_company_email():
    sql1 = f"""select count(1) from algo_service_company_email;"""
    sql_status1 = utils.execute_query(client=redshift_client, db_name=DB_NAME, sql=sql1)
    id = sql_status1['response']['Id']
    sql_result1 = utils.get_query_result(redshift_client, id)
    count_company_email = sql_result1["Records"][0][0]['longValue']
    print('count_company_email: ', count_company_email)
    return count_company_email

def is_file_exist(filename):
    for bing_prefix in ALL_BING_SRC_PREFIX:
        prefix = bing_prefix + filename
        print(prefix)
        res = s3_client.list_objects_v2(Bucket=BING_BUCKET_NAME, Prefix=prefix, MaxKeys=1)
        print(res)
        if 'Contents' in res:
            print ("bing_prefix:", bing_prefix)
            return bing_prefix
    return ""

def process_file(filename):
    print('******** create schema of table ***************')
    create_schema(table = 'algo_service_bing_company_to_domain')
    create_schema(table = 'algo_service_company_email')

    print(":::::filename:::::", filename)
    bucket = BING_BUCKET_NAME
    prefix = is_file_exist(filename)
    if prefix:
        key = prefix + filename
        status = STATUS_INSERT_INPROGRESS
    else:
        status = STATUS_NOT_FOUND

    con, cur = utils.get_db_con()
    now = datetime.datetime.now()
    data = {}
    data['s3_path'] = f's3://{bucket}/{prefix}'
    data['filename'] = filename
    data['file_key'] = 'BING'
    data['total_records'] = 0
    data['created_at'] = now
    data['updated_at'] = now
    data['status'] = status
    data['comment'] = ''
    print("insert_data: ", data)
    record_id = helper.insert_inprogress(cur, data)
    if status == STATUS_NOT_FOUND:
        return

    resp = s3_client.get_object(Bucket=bucket, Key=key)
    row_count = 0
    i = 0
    
    try:
        for df in pd.read_csv(BytesIO(resp['Body'].read()), on_bad_lines='skip', chunksize=50000, encoding='latin'):
            if not verify_column_bing(df):
                status, comment = STATUS_INVALID, 'invalid header'
                print('Invalid columns')
                break
            else:
                print("-"*20, i, "-"*20)
                        
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                csv_buf_1 = StringIO()
                row_company_domain_writer = csv.writer(csv_buf_1)
            
                csv_buf_2 = StringIO()
                row_company_email_writer = csv.writer(csv_buf_2)

                for index, row in df.iterrows():
                    csv_company = row['Keyword']
                        
                    try:
                        csv_mails = json.loads(row['Emails'])
                        if csv_mails:
                            for email in csv_mails:
                                row_company_email_writer.writerow([csv_company, email])
                        else:
                            row_company_email_writer.writerow([csv_company, None])
                        
                    except:
                        pass
                        
                    if row["Organic_Results"]:
                        try:
                            list_json = json.loads(row["Organic_Results"])
                            for dictValues in list_json:
                                row_company_domain_writer.writerow(
                                    [
                                        csv_company,
                                        dictValues.get('created_at', now),
                                        dictValues.get('position'),
                                        dictValues.get('displayedLink'),
                                        dictValues.get('title'),
                                        dictValues.get('snippet'),
                                        dictValues.get('domain'),
                                        None,
                                        None,
                                        None,
                                        None,
                                        None,
                                    ]
                                )
                        except json.JSONDecodeError as e:
                               print(f"Error: {e}")
                               print(row["Organic_Results"])
                row_count += df.shape[0]
                row_counts = save_to_s3_company_domain(bucket=BING_BUCKET_NAME, key=f"{BING_OUTPUT_COMPANY_DOMAIN}{filename.split('.')[0]}_{i}.csv", data=csv_buf_1)
                move_s3_to_redshift(bucket=BING_BUCKET_NAME, key=f"{BING_OUTPUT_COMPANY_DOMAIN}{filename.split('.')[0]}_{i}.csv", table='algo_service_bing_company_to_domain')
                
                if csv_buf_2.getvalue() != '':
                    row_counts = save_to_s3_company_email(bucket=BING_BUCKET_NAME, key=f"{BING_OUTPUT_COMPANY_EMAIL}{filename.split('.')[0]}_{i}.csv", data=csv_buf_2)
                    move_s3_to_redshift(bucket=BING_BUCKET_NAME, key=f"{BING_OUTPUT_COMPANY_EMAIL}{filename.split('.')[0]}_{i}.csv", table='algo_service_company_email')
                comment = f'company_to_domain: {get_company_to_domain()}, company_email: {get_company_email()}'
                status = INSERT_COMPLETED
                i+=1
    except Exception as err:
        status, comment = STATUS_INVALID, f'{err}'
        traceback.print_exc()  
    
    helper.update_status(cur=cur, id=record_id, total_records=row_count, status=status, comment=comment)


    con.close()