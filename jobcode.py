import json
import boto3
import time

def lambda_handler(event, context):
    client1 = boto3.client('logs',region_name='us-east-2')
    Client = boto3.client('glue',region_name='us-east-2')
    try:
        myNewJobRun = Client.start_job_run(JobName='ctg-fis-job-razee')
    except Client.exceptions.InvalidInputException:
        print("Nameerror:invalid input")
    except Client.exceptions.ResourceNumberLimitExceededException:
        print("errorname:ResourceNumberLimitExceededException")
    except client.exceptions.ConcurrentRunsExceededException:
        print("errorname:ConcurrentRunsExceededException")
    except Exception as ex:
        print(ex)
    else:
        runId =myNewJobRun['JobRunId']
        print("job is running it takes few minute to complete wait...")
        statusupdate = client.get_job_run(JobName='ctg-fis-job-razee',RunId='runid')
        
        while(statusupdate['JobRun']['JobRunState']==('STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'FAILED'|'TIMEOUT')):
            if(statusupdate=='STARTING'):
                print("Job Starting")
            elif statusupdate=='RUNNING':
                time.sleep(30)
                print("status:Glue Job Running")
            elif statusupdate=='STOPPING'|'STOPPED':
                print("status:STOPPING'|'STOPPED ")
            elif statusupdate =='FAILED'|'TIMEOUT':
                print("status:failed or timeout")
            else:
                print("status:completed\SUCCEEDED!sending data to redshift")
                #creating a logstream
                logname="FIS-Razik"
                create_logs = client1.create_log_stream(logGroupName='/aws-glue/jobs/'+logname,logStreamName=runId)
                if(create_logs!=''):
                    print("logstream not created")
                else:
                    print("Logstream : " + runId + " is created")
        else:
            print("unlisted status")    
