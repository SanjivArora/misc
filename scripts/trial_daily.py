# This script:
# -Extracts relevant @Remote data to project share
# -Starts production AWS server
# -Syncs data to S3
# -Updates prediction source code on remote server
# -Trains model on remote server
# -Predicts with trained model on remote server
# -Syncs prediction results to trial share
# -Generates aligned dataset and cached data for models
#
# Note that the environment must be set up correctly, specifically:
# -Windows host (cygwin logic would not be required on *nix systems)
# -AWS API configured with credentials and correct default region, with boto3 and awscli libraries installed
# -Access to shares
# -Cygwin SSH set up for key-based authentication to remote server as rstudio user
# -Cygwin SSH accepts the remote server as a known host

import subprocess
import time
import traceback
import sys

import boto3
import awscli
import awscli.clidriver

from data_download_s3 import copy_all
import sync_dispatch


# Prod server instance ID in AWS
instance_id = 'i-00e8df1734a9e2177'

cyg_bash = r'c:\cygwin64\bin\bash.exe'
ssh = '/usr/bin/ssh'

device_groups = [
    'trial_prod',
    'trial_commercial',
]


# Run a command as if we are logged in to the cygwin environment
# cyg_run and remote_run don't correctly escape input, for the intended use this isn't a problem.
def cyg_run(cmd_args, check_retcode=True):
    # Quote arguments
    cmd_args = ['"%s"' % x for x in cmd_args]
    cmd = [cyg_bash, '-l', '-c', " ".join(cmd_args)]
    print("running: %s" % cmd)
    res = subprocess.call(cmd, stderr=subprocess.STDOUT)
    if(check_retcode and res!=0):
        raise(ChildProcessError("%s returned nonzero retcode %i" % (cmd_args, res)))
    return(res)

def run_cmd(cmd, check_retcode=True, stdout=sys.stdout):
    # Quote arguments
    print("running: %s" % cmd)
    res = subprocess.call(cmd, stderr=subprocess.STDOUT, stdout=stdout)
    if(check_retcode and res!=0):
        raise(ChildProcessError("%s returned nonzero retcode %i" % (cmd, res)))
    return(res)

def remote_run(instance, cmd_args, check_retcode=True):
    cmd = [
        ssh,
        '-p 443',
        'rstudio@%s' % instance.public_ip_address,
    ] + cmd_args
    return(cyg_run(cmd))

# Get Boto instance for AWS prod server
def get_instance(id=instance_id):
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.filter(InstanceIds=[instance_id])
    instances = list(instances)
    if(len(instances)==0):
        raise(IndexError("AWS instance %s not found" % instance_id))
    instance = instances[0]
    return(instance)

def start_remote(instance):
    print("Starting remote server")
    instance.start()

def stop_remote(instance):
    print("Stopping remote server")
    instance.stop()
    
def extract_data():
    print("Extracting latest @Remote data")
    copy_all()

def wait_for_boot():
    print("Waiting for remote server to be ready")
    time.sleep(180)

def train(instance, device_group):
    cmd=["cd ~/prediction && Rscript scripts/train.R --device_group %s" % device_group]
    remote_run(instance, cmd)

def predict(instance, device_group):
    #cmd=["cd ~/prediction && Rscript scripts/predict.R --device_group %s --email_to=testing@sdmatthews.com" % device_group]
    cmd=["cd ~/prediction && Rscript scripts/predict.R --device_group %s" % device_group]
    remote_run(instance, cmd)

# Sync predictions from S3
def sync_predictions():
    # Use AWS CLI implementation directly
    driver = awscli.clidriver.create_clidriver()
    driver.main([
        "s3",
        "sync",
        "s3://ricoh-prediction-results/",
        r"\\ricohdc-data\MachineLearning$\Machine Learning\Trial Predictions"
    ])

def update_prediction_src(instance):
    cmd=["cd ~/prediction; git pull"]
    remote_run(instance, cmd)

def update_toner_src(instance):
    cmd=["cd ~/toner; git pull"]
    remote_run(instance, cmd)

def generate_dataset(instance):
    cmd=["cd ~/prediction; Rscript scripts/align_data.R --incremental; Rscript scripts/generate_dataset.R"]
    remote_run(instance, cmd)

def toner_prediction_auckland(instance):
    cmd=["cd ~/toner; python3 scripts/predict_auckland_uni.py"]
    remote_run(instance, cmd)

def toner_prediction_fleet(instance):
    cmd=["cd ~/toner; python3 scripts/predict_fleet.py"]
    remote_run(instance, cmd)

def predict_toner_dispatch(instance):
    cmd=["cd ~/toner; python3 scripts/predict_dispatch.py"]
    remote_run(instance, cmd)

def sync_toner_dispatch(instance):
    sync_dispatch.sync_dispatch()

def run():
    instance = get_instance()
    try:
        start_remote(instance)
        extract_data()
        wait_for_boot()
        try:
            update_prediction_src(instance)
        except:
            traceback.print_exc()            
##        for device_group in device_groups:
##            try:
##                train(instance, device_group)
##            except:
##                traceback.print_exc()
##            try:
##                predict(instance, device_group)
##            except:
##                traceback.print_exc()
        for f in [update_toner_src, generate_dataset, toner_prediction_auckland, toner_prediction_fleet, predict_toner_dispatch, sync_toner_dispatch]:
            try:
                f(instance)
            except:
                traceback.print_exc()
    # Ensure that we shut down the remote server if we hit an unhandled exception
    finally:
        stop_remote(instance)
    #sync_predictions()

if(__name__=='__main__'):
    run()