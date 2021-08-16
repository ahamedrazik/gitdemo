import json
import boto3
import time

def lambda_handler(event, context):
    client1 = boto3.client('logs',region_name='us-east-2')
    Client = boto3.client('glue',region_name='us-east-2')
    try:
        myNewJobRun = Client.start_job_run(JobName='ctg-fis-job-razee')
        time.sleep(30)
        print("RunID for ctg-fis-job-razee is: ",myNewJobRun['JobRunId'])
        run="myNewJobRun['JobRunId']"
        print("sending logs to logstream...")
    except Exception as ex:
        print(ex)
        
    else:
        gluename="FIS-Razik"
        response_create_logs = client1.create_log_stream(logGroupName='/aws-glue/jobs/' + gluename,logStreamName=myNewJobRun['JobRunId'])
        print("logstream " + myNewJobRun['JobRunId'] + "is created")
        
        errortaking = Client.get_job_run(JobName='ctg-fis-job-razee',RunId='jr_36d2dc1207621818b28299672fee9649c7037290e16fc962cc89a1a5caddc629')
        if(errortaking['JobRun']['ErrorMessage'][0]!=0):
            print("ERROR IS:",errortaking)
            try:
                err_log = errortaking['JobRun']['ErrorMessage']
                response = client1.put_log_events(logGroupName='/aws-glue/jobs/' + gluename ,logStreamName=myNewJobRun['JobRunId'],
                logEvents=[
                    {
                        'timestamp': int(round(time.time() * 1000)),
                        'message': err_log
                    }])
            except Exception as ex:
                print(ex)
        else:
             print("jobsucessfullycompleted")