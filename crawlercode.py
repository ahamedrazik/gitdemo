import json
import boto3
import time
def lambda_handler(event, context):
    print(event)
    s3=boto3.client('s3')
    Client = boto3.client('glue')
    try:
        response = Client.start_crawler(Name='poc_output')
        time.sleep(30)
    except Client.exceptions.EntityNotFoundException:
        print("EntityNotFoundException")
    except Client.exceptions.CrawlerRunningException:
        print("CrawlerRunningException")
    except Client.exceptions.OperationTimeoutException:
        print("OperationTimeoutException")
    else:
        response1 = client.get_crawler(Name='poc_output')
        if(response1['Crawler']['State']=='RUNNING'):
            print("crawler is running to crawl the data")
        else:
            (response1['Crawler']['State']=='STOPPING')
            print("crawler is stopping ")
            time.sleep(30)
            print("function invoke other function when crawler is successed")
            
            
    