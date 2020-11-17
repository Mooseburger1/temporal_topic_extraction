import boto3, botocore
import argparse
import csv
import logging
import os
import sys
from colorlog import ColoredFormatter
import warnings
from multiprocessing import Process, Queue
import time

config = botocore.config.Config(retries=dict(max_attempts=50))

def check_bucket(bucket):
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
        return True
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            streamLogger.critical("Private Bucket. Forbidden Access!")
            return False
        elif error_code == 404:
            streamLogger.critical("Bucket Does Not Exist!")
            return False

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

def read_file(s3, bucket, file):
    csv_file = s3.get_object(Bucket=bucket, Key=file)
    record_list = csv_file['Body'].read().decode('utf-8').split('\n')
    csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
    next(csv_reader)
    return csv_reader

def write(dynamodb, batch, process_num, num):
    streamLogger.info('Process: {} | BatchNo: {}'.format(process_num, (num+1)/BATCH))
    response = dynamodb.batch_write_item(RequestItems=batch)
    streamLogger.info('Status: {} | Unprocessed: {}'.format(response['ResponseMetadata']['HTTPStatusCode'] , response['UnprocessedItems']))
    return response['UnprocessedItems']

def write_to_dynamo(dynamodb, region, table, csv_reader, file, process_num):
    items = []
    for num, row in enumerate(csv_reader):
        if not row: continue

        uid = row[0]
        year = row[2]
        pub = row[8]
        bow = row[9]

        item = {'PutRequest':{'Item':{
                        'uid' : {'S' : str(uid)},                                       #write 'uid' (sort key)
                        'year' : {'N' : str(year)},                                     #write 'year' (partition key)                                  
                        'bow' : {'S' : str(bow)},                                       #write attribute 'bow'
                        'publication' : {'S' : str(pub)},                               #write 'publication' attribute
                        'file': {'S' : str(file)}                                       #write attribute 'file'
                    }}}
        
        items.append(item)

        if (num + 1) % BATCH == 0:
            batch = {table : items}

            success = False
            while not success:
                try:
                    unprocessed = write(dynamodb, batch, process_num, num)
                    if unprocessed == {}:
                        success = True
                        items=[]
                    else:
                        success = False
                        streamLogger.critical('Unprocessed data in {} batch {}'.format(process_num, (num+1)/BATCH))
                except Exception as e:
                    streamLogger.warning('Exception while writing {} batch {}'.format(process_num, (num+1)/BATCH))
                    streamLogger.warning(e)
                    time.sleep(30)
                
            if (num + 1) % 100==0:
                streamLogger.warning('Process {} has written 100 - sleeping 30s'.format(process_num))
                time.sleep(30)

            
    if items:
        batch = {table : items}
        write(dynamodb, batch, process_num, num)  
    streamLogger.info('PROCESS {} FINISHED!!!!!'.format(process_num))

def s3_to_dynamodb(key, secret, bucket, region, table, file, process_num):

    s3 = boto3.client(
                      service_name='s3', 
                      aws_access_key_id=key, 
                      aws_secret_access_key=secret
        )

    dynamodb = boto3.client(
                            service_name='dynamodb',
                            aws_access_key_id=key,
                            aws_secret_access_key=secret,
                            verify=False,
                            region_name=region,
                            config=config
        )

    streamLogger.info('Spawned Process [{}] reading file {}'.format(process_num,file))
    csv_reader = read_file(s3, bucket, file)
    streamLogger.info('Spawned Process [{}] read successful'.format(process_num))
    write_to_dynamo(dynamodb, region, table, csv_reader, file, process_num)

############### Logging Configuration ###############
streamFormatter = "%(asctime)3s  %(log_color)s%(levelname)-3s%(reset)s %(white)s | %(message)s%(reset)s"
streamFormatter = ColoredFormatter(streamFormatter)

#register logging stream
streamLogger = logging.getLogger(__name__)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(streamFormatter)
consoleHandler.setLevel(logging.INFO)

streamLogger.addHandler(consoleHandler)
streamLogger.setLevel(logging.INFO)

############### CLI ARGS ###############
parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bucket', dest='bucket', help='S3 buckets where CSV files are located', default=None)
parser.add_argument('-t', '--table', dest='table', help='DynamoDB table where data will be written', default=None)
parser.add_argument('-r', '--region', dest='region', help='AWS Region resources are located in', default='us-east-1')
args = parser.parse_args()

BUCKET = args.bucket
TABLE = args.table
REGION = args.region
BATCH = 25
############### AWS CREDENTIALS ###############
KEY = os.environ['AWS_KEY']
SECRET = os.environ['AWS_SECRET']

#obfuscate for logging purposes
OBFUSCATED_KEY = '#' * (len(KEY) - 3) + KEY[-3:]
OBFUSCATED_SECRET = '#' * (len(SECRET) -5 ) + SECRET[-5:]

if __name__ == '__main__':

    if BUCKET == None:
        streamLogger.critical('No S3 bucket provided')
        sys.exit()

    if TABLE == None:
        streamLogger.critical('No DynamoDB table provided')
        sys.exit()


    #output params being used
    streamLogger.info('\n')
    streamLogger.info('Bucket: {}'.format(BUCKET))
    streamLogger.info('Table: {}'.format(TABLE))
    streamLogger.info('Region: {}'.format(REGION))
    streamLogger.info('Key: {}'.format(OBFUSCATED_KEY))
    streamLogger.info('Secret: {}'.format(OBFUSCATED_SECRET))


    try:
        s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET,
            verify=False
        )

        if not check_bucket(BUCKET):
            sys.exit()
        bucket = s3.Bucket(BUCKET)

    except Exception as e:
        streamLogger.critical('Failed to connect to S3 Services due to following exception: {}'.format(e))
        sys.exit()


    objs = bucket.objects.filter(Prefix='clean2')
    csv_files = [file.key for file in objs if file.key.endswith('.csv')]
    
    if len(csv_files) == 0:
        streamLogger.warning('No csv files found')
        sys.exit()
    
    streamLogger.info('Found [{}] csv files'.format(len(csv_files)))

    processes=[]

    for num, file in enumerate(csv_files):
        p = Process(target=s3_to_dynamodb, args=(KEY, SECRET, BUCKET, REGION, TABLE, file, num))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    streamLogger.info('Write to DynamoDB Complete!')