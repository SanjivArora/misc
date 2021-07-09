# Copy data files to S3
# Note that the AWS client API must be configured with access credentials and the correct default region

import os
from zipfile import *
import io
import shutil
import boto3
import tempfile
import bz2
import re
import functools
import concurrent.futures
import threading
import datetime
import traceback

source = r"\\ricohsql-man\OutPath"
# Destination S3 Bucket
dest = 'ricoh-prediction-data'
days = 7
today = datetime.datetime.now().date()
matching_dates = [today - datetime.timedelta(days=i) for i in range(0,days)]
matching_zipfiles = ["%s.zip" % (date.strftime('%Y%m%d')) for date in matching_dates]

nthreads = 20

s3_client=boto3.resource('s3')
bucket=s3_client.Bucket(dest)

#####################################
### Efficiently query pre-existing files at destination
### Files must be stored as <date>/<name>
#####################################

def memoize(func):
    cache = func.cache = {}
    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]
    return memoized_func

# Find first date in YYYYMMDD format in string (simple implementation - find 8 consecutive digits)
def get_date(name):
    match = re.search(r'\d{8}', name)
    if not match:
        return None
    return match.group()

@memoize
def s3_files_for_path_inner(path, bucket=bucket):
    files = set([x.key for x in bucket.objects.filter(Prefix=path)])
    return(files)

# Lock access to this function to avoid race condition for calculating update to memoization
lock = threading.Lock()
def s3_files_for_path(*args, **kwargs):
    with lock:
        res = s3_files_for_path_inner(*args, **kwargs)
        return(res)

def s3_file_exists(path, bucket=bucket, verbose=False):
    date = get_date(path)
    files = s3_files_for_path(date, bucket)
    res = path in files
    if res and verbose:
        print("%s already exists at the destination" % path)
    return(res)

def canonicalize(name):
    date = get_date(name)
    if not date:
        return(None)
    res = date + "/" + name
    return(res)
    
def s3_file_exists_noprefix(name):
    path = canonicalize(name)
    res = s3_file_exists(path)
    return(res)
    

#####################################
### Compress to bz2 and write to S3 under <date>/<name>.bz2
#####################################

def compress_and_write(contents, name, bucket=bucket):
    contents = bz2.compress(contents)
    path = canonicalize(name)
    if not name:
        print("Skipping %s - date extraction failed" % name)
        return()
    if not path.endswith(".bz2"):
        path = path + ".bz2"
    if not s3_file_exists(path, bucket):
        print("Writing %s to S3" % path)
        bucket.put_object(Key=path, Body=contents)

#####################################
### Extracting data from zipfiles ###
#####################################

def process_file_from_zip(zipfile, name):
    newname = name + ".bz2"
    if not canonicalize(name):
        print("Skipping %s - date extraction failed" % name)
        return()
    if not s3_file_exists_noprefix(newname):
        try:
            contents = zipfile.read(name)
            compress_and_write(contents, newname)
        except BadZipFile:
            print("%s is not a valid zip file, skipping" % name)
                
def csv_extract(zipfile):
    def process_file(name):
        return(process_file_from_zip(zipfile, name))
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
        # Retrieve values to ensure exceptions are re-raised
        [x for x in executor.map(process_file, zipfile.namelist())]

def file_extract(path):
    print("Extracting files from %s" % path)
    try:
        z = ZipFile(path,'r')
    except BadZipFile:
        print("%s is not a valid zip file, skipping" % path)
        return(False)
    files = z.namelist()
    for name in files:
        if(name.endswith('.zip')):
            p2 = z.open(name)
            f2 = io.BytesIO(p2.read())
            try:
                z2 = ZipFile(f2)
                csv_extract(z2)
                z2.close()
            except BadZipFile:
                print("%s in %s is not a valid zip file, skipping" % (name, path))
            finally:
                p2.close()
                f2.close()
        else:
            # Top-level @Remote files
            process_file_from_zip(z, name)
    z.close()

def copy_from_zip():
    # Clear destination file cache
    s3_files_for_path_inner.cache.clear()
    for root, dirs, files in os.walk(source):
        print("Extracting zipped files from %s" % (root))
        def process_zipfile(file):
            path = os.path.join(root, file)
            file_extract(path)
        for file in files:
            if file in matching_zipfiles:
                process_zipfile(file)

####################################
### Extracting data from folders ###
####################################

def copy_from_folders():
    # Clear destination file cache
    s3_files_for_path_inner.cache.clear()
    for root, dirs, files in os.walk(source):
        print("Copying unzipped files from %s" % (root))
        def process_file(file):
            if file.endswith('.csv') and not s3_file_exists(canonicalize(file+".bz2")):
                source_path = os.path.join(root, file)
                try:
                    f = open(source_path, 'rb')
                    contents = f.read()
                    f.close()
                    compress_and_write(contents, file)
                except:
                    print("Error processing %s: %s" % (source_path, traceback.format_exc()))     
        with concurrent.futures.ThreadPoolExecutor(max_workers=nthreads) as executor:
            # Retrieve values to ensure exceptions are re-raised
            [x for x in executor.map(process_file, files)]


####################################
### If running this as a script, do the thing
####################################

def copy_all():
    # Run twice to handle files that are moved while running
    copy_from_folders()
    copy_from_folders()
    
    copy_from_zip()
    
if __name__=='__main__':
    copy_all()
