import json
import os
import gzip
import boto3
from datetime import datetime
import pandas as pd
import awswrangler as wr
import pytz


s3 = boto3.resource('s3')
read_schema = ['log_timestamp','log_type','log_message','process_timestamp','broker','cluster','dt']
write_schema = {column: 'string' for column in read_schema}
    

def get_now_string():
    now = datetime.now().astimezone(pytz.timezone(os.environ['PYTZ_TIMEZONE']))
    return now.strftime('%Y-%m-%d %H:%M:%S')


def get_log_type(log):
    if 'INFO' in log[26:35]:
        return 'INFO'
    if 'DEBUG' in log[26:35]:
        return 'DEBUG'
    if 'ERROR' in log[26:35]:
        return 'ERROR'
    else:
        return 'NOT IDENTIFIED'


def get_log_msg(log,log_type):
    if log_type in log:
        return log[log.find(log_type)+len(log_type)+1:]
    else:
        return log


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        obj_key = record['s3']['object']['key']

        print(f'processing the bucket {bucket} | object key: {obj_key}')

        obj = s3.Object(bucket,obj_key)
        with gzip.GzipFile(fileobj=obj.get()['Body']) as gzipfile:
            content = gzipfile.readlines()
        
        try:
            broker_name = obj_key.split('/')[-1].split('_')[0]
            process_datetime = get_now_string()
            cluster_name = obj_key.replace(os.environ['SOURCE_MSK_LOGS_PREFIX'],'').split('/')[0]

            logs = [line.decode().replace('\n','') for line in content]
            
            parsed_logs = []
            for log in logs:
                log_timestamp = log[1:24].replace(',','.')
                log_type = get_log_type(log)
                log_message = get_log_msg(log,log_type)
    
                parsed_logs.append(
                    [
                        log_timestamp,
                        log_type,
                        log_message,
                        process_datetime,
                        broker_name,
                        cluster_name,
                        log_timestamp[:10]
                    ]
                )
        except e:
            print(f'An exception happened: {e}')
        else:
            logs_df = pd.DataFrame(parsed_logs, columns=read_schema)

            wr.s3.to_parquet(
                df=logs_df,
                dtype=write_schema,
                path=f"s3://{os.environ['TARGET_S3_BUCKET']}/{os.environ['TARGET_MSK_LOGS_PREFIX']}",
                dataset=True,
                database=os.environ['GLUE_DB'],
                table=os.environ['GLUE_TABLE'],
                mode='append',
                partition_cols=['cluster','broker','dt'],
                compression='snappy'
            )
            print(f"logs parsed and saved into s3://{os.environ['TARGET_S3_BUCKET']}/{os.environ['TARGET_MSK_LOGS_PREFIX']}cluster={cluster_name}/broker={broker_name}/")
    
    
    return {
        'statusCode': 200
    }