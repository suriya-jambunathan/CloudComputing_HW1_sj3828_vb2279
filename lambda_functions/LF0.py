import json
import boto3
import datetime
import uuid

def generate_session_id():
    # Generate a unique identifier for the session
    unique_identifier = str(uuid.uuid4())
    
    # Generate the timestamp when the conversation starts
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Combine the timestamp and the unique identifier
    session_id = f"{timestamp}-{unique_identifier}"

    return session_id

def lambda_handler(event, context):

    # Connect to lex and return response
    client = boto3.client('lex-runtime')
    print("event is: ", event)
    user_id = 'user1'
    bot_name_lex = 'DiningBot'
    bot_alias =  'stg'
    msg_text = event['messages'][0]['unstructured']['text']
    try:
        session_id = event['messages'][0]['unstructured']['id']
    except:
        session_id = generate_session_id()
    response = client.post_text(
    botName=bot_name_lex ,
    botAlias= bot_alias,
    userId=user_id,
    sessionAttributes={
        'string': 'string',
        'sessionId': session_id
    },
    requestAttributes={
        'string': 'string'
    },
    inputText= msg_text
    ) 
    bot_response= {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                    "id": 'User1',
                    "text": response['message'],
                    "timestamp": "",
                    'sessionId': session_id
                                     }
                }
                         ]

            }
    return bot_response