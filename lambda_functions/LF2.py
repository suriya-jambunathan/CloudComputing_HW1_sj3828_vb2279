import boto3
import sys
import os
import json
import boto3
#import logger
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from boto3.dynamodb.conditions import Key
import requests

#Send email for SES
def send_email(sender_email, message):
    msg = MIMEMultipart()
    msg["Subject"] = "Restaurant recommedation for you!"
    msg["From"] = "vamsikrishh0099@gmail.com"
    msg["To"] = "vamsikrishh0099@gmail.com"

    # Set message body
    body = MIMEText(message)
    msg.attach(body)


    # Convert message to string and send
    ses_client = boto3.client("ses", region_name="us-east-1")
    
    response = ses_client.send_raw_email(
        Source="vamsikrishh0099@gmail.com",
        Destinations=["vamsikrishh0099@gmail.com"],
        RawMessage={"Data": msg.as_string()}
    )
    
    response = ses_client.send_raw_email(
        Source="suriya.jambunathan@gmail.com",
        Destinations=["suriya.jambunathan@gmail.com"],
        RawMessage={"Data": msg.as_string()}
    )

    return response



#Get Restaurant from Elastic Search
def elastic_search_id(cuisine):
    headers = {'content-type': 'application/json'}
    
    esUrl = 'https://search-restaurants-es-*****.us-east-1.es.amazonaws.com/_search?q=cuisine:'+cuisine+ '&size=5'
  
    esResponse = requests.get(esUrl, auth=("<username>", "<pwd>"), headers=headers)
   
    #logger.debug("esResponse: {}".format(esResponse.text))
    data = json.loads(esResponse.content.decode('utf-8'))
    # print(data)
    # print(type(data))
    #logger.info("data: {}".format(data))
    try:
        esData = data["hits"]["hits"]
        #print(esData)
        return esData
        #logger.info("esData: {}".format(esData))
    except Exception as e:
        print(e)
        raise e 
        #es_id="10tnq8x2qI7ix7VqVP0rMw"
        #return es_id


#Get Data from DynamoDB
def query_data_with_sort(es_id,cuisine):
    
    print("dynamodb query:")
    table_name = "restaurants"
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(table_name)

    try:
        # response = table.query(
        #     KeyConditionExpression=Key('business_id').eq(es_id)
            
        # )
        response = table.query(
    KeyConditionExpression=Key('cuisine').eq(cuisine) & Key('business_id').eq(es_id)
    )

    except Exception as e:
        print(f"ParamValidationError: {e}")
        return None
    # print(type(response["Items"]))
    # print(response["Items"])
    if response and len(response["Items"]) > 0:
        return response['Items'][0]
    else:
        return []
    
  
# Receive message from SQS
sqs_client = boto3.client("sqs", region_name="us-east-1")

def receive_message(response):
    
    
    print(response)
    if not (response and "Records" in response):
        print("no messages")
        return None, None, None
    else:
        
        
        for message in response.get("Records", []):
            #print("test")
            message_body = message["body"]
            print(message)
            # print(f"Receipt Handle: {message['ReceiptHandle']}")
            indi_msg = json.loads(message_body)
            cuisine = indi_msg['cuisine']
            
            customer_phone = indi_msg['phone']
            customer_email = indi_msg['email']
            no_ppl = indi_msg["num_ppl"]
            time = indi_msg["time"]
            date = indi_msg["date"]
            Message_to_send = "Hello! Here are some "+ cuisine +" restaurant suggestions for " +str(no_ppl)+ " people, for "+ str(date)+ "\n"+ " at "+ str(time)  + " Enjoy your meal!"
            #print(message)
            es_data=elastic_search_id(cuisine)
            print("es is done")
            j=0
            for i in range(0,len(es_data)):
                
                es_id = es_data[i]['_source']["business_id"]
                # print(es_id)
                item_db = query_data_with_sort(es_id,cuisine)
                
                if len(item_db) >0:
                    # print(j)
                    j+=1
                    if 'phone' in item_db.keys():
                        restaurant_phone = str(item_db['phone'])
                    else:
                        restaurant_phone = 'NA'
                    addr = str(item_db['address'])
                    Message_to_send += "\n"+ str(j) +".  "+ str(item_db['name']) + " at " + addr
            
            
            #print(Message_to_send)
            
            sqs_client.delete_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/*****/restaurant_request",
                ReceiptHandle=message['receiptHandle']
            )
            response = send_email(customer_email, Message_to_send)
            print("Email Sent")
    return Message_to_send, customer_phone, customer_email


def lambda_handler(event, context):
    try:
        print(event)
        message_to_send, customer_phone, customer_email = receive_message(event)
       
            
        #print(customer_email)
        
        
        
       
    except Exception as e:
        raise e