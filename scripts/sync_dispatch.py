# Sync current predictive toner dispatch files from S3
# Note that the AWS client API must be configured with access credentials and the correct default region

import os
import re
import datetime
import traceback
import tempfile
import concurrent.futures

import boto3


# Predictive Toner Dispatch S3 Bucket
bucket = 'ricoh-prediction-dispatch'

dest = r"\\ricohsql-man\ASP"

s3_client=boto3.client('s3')
print(type(s3_client))

nthreads=20

# https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
def S3Keys(bucket_name, prefix='/', delimiter='/', start_after=''):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    s3_client = boto3.client('s3')
    s3_paginator = s3_client.get_paginator('list_objects_v2')
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            yield content['Key']

def sync_serials(datestring):
    print(bucket)
    keys = S3Keys(bucket, prefix=f'predictive-serials/{datestring}/')
    keys = list(keys)
    if not keys:
        raise("No predictive serials file")
    destfile= os.path.join(dest, 'PredictiveSerialNumbers', 'PredictiveSerialNumbers.xml')
    print(f"Syncing serials file from {keys[0]} to {destfile}")
    s3_client.download_file(bucket, keys[0], destfile)

def sanitize_filename(filename):
    return "".join([c for c in filename if c.isalpha() or c.isdigit() or c in [' ', '.', '-', '_']])

def copy_order(order_key):
    name = order_key.split('/')[-1]
    s3_client.download_file(bucket, order_key, os.path.join(dest, sanitize_filename(name)))

def sync_orders(datestring):
    keys = S3Keys(bucket, prefix=f'orders/{datestring}/')
    keys = list(keys)
    print(f"Syncing {len(keys)} orders")
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
       # Retrieve values to ensure exceptions are re-raised
       [x for x in executor.map(copy_order, keys)]

def sync_dispatch():
    today = datetime.datetime.now().date()
    datestring = today.strftime('%Y%m%d')
    sync_serials(datestring)
    sync_orders(datestring)


if __name__=='__main__':
    sync_dispatch()