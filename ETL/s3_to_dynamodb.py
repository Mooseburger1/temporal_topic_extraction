import boto3, botocore
import argparse
import csv
import logging
import os
import sys
from colorlog import ColoredFormatter
import warnings
import multiprocessing

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


        
    csv_files = [file.key for file in bucket.objects.all() if file.key.endswith('.csv')]
    
    if len(csv_files) == 0:
        streamLogger.warning('No csv files found')
        sys.exit()
    
    streamLogger.info('Found [{}] csv files'.format(len(csv_files)))