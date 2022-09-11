import json
import boto3
import sys
import os
from webex_bot.webex_bot import WebexBot  ###
def send_notifications(row):   

    """
    To do:
        Send webex message to channel
    
    Args:
        dag_name (str): Required statement/message
       
    Returns:
        None 
        
    """         
    webex_token = "#########ENTER GENERATED BOT TOKEN HERE"
    room_token = "#########ENTER ROOM TOKEN HERE"
    bot = WebexBot(webex_token)
    notification = row
    bot.send_message_to_room_or_person(None, room_token, False, False,notification)
    
    

def lambda_handler(event, context):    
    print("event received is :", event)
    
    delta = int(event["time_calculated"])
    last_execution_date= event["last_execution_date"]
    last_execution_status = event["last_execution_status"]
    dag_id = event["dag_id"]
    run_id = event["run_id"]
    
    stmt = f" Time since last successful dag execution is {delta} hour/s \n "
    if delta == 1:
        stmt +=  "** MWAA is Operational**  \n"
    else:
        stmt += " **NO HEART BEAT RECEIVED, CHECK HEALTH STATUS MANUALLY** \n" 
        
    stmt += f"Last run details of Dag  '{dag_id}' are as follows : \n  last execution date :  {last_execution_date} \n last execution status: **{last_execution_status}** \n last run id : {run_id}"
    print ("final stmt",stmt)
    print ("sending notification now")
    
    send_notifications(stmt)
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
