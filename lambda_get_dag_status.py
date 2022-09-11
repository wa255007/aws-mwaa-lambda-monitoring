import json
import math
import boto3
import base64
import ast
import http.client
from datetime import datetime
import pandas as pd

mwaa_env_name = "####Enter Mwaa environmentname"

def get_last_run_info_mwaa(dag_name:str):
    """
    To do:
        Get last execution information of given dag name
    
    Args:
        dag_name (str): Required dag name to return the details
       
    Returns:
        max_value : maximum date value for last execution 
        last_execution_status : Return latest row returned as dictionary to extract required infromation  
        
    """
    client = boto3.client('mwaa')
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    payload = f"dags list-runs -d {dag_name} -o json"
    print ("payload is ",payload)
    headers = {
        'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
        'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    output = base64.b64decode(mydata['stdout'])
    error = base64.b64decode(mydata['stderr'])
    dict_temp=json.loads(output.decode("utf-8"))
    df = pd.DataFrame(dict_temp)
    column = df["start_date"]
    max_value = column.max()
    #Coverting latest row value in dictionary with full details
    last_execution_status=df.iloc[0].to_dict()
    return max_value,last_execution_status



def lambda_handler(event, context):
    
    current_date = datetime.utcnow()
    #call function get last execution status
    max_value,last_execution_status = get_last_run_info_mwaa()
    #reformating the maximum timestamp value to find out the difference with current date
    date_path = datetime.strptime(max_value,'%Y-%m-%dT%H:%M:%S.%f+00:00')
    delta = math.ceil ((current_date-date_path).total_seconds()/3600)
    #Creating payload for passing to another lambda   
    payload= { "time_calculated" : json.dumps(delta), "last_execution_date" : last_execution_status["start_date"] ,
    "last_execution_status":last_execution_status["state"] , "dag_id" :last_execution_status["dag_id"] , "run_id" :last_execution_status["run_id"] }
    
    lambda_client = boto3.client('lambda', region_name='###REPLACE REGION NAME')
    function_name="######AFTER CREATING SECOND LAMBDA REPLACE NAME OF NEW LAMBDA HERE"    
    response = lambda_client.invoke(FunctionName=function_name,InvocationType="RequestResponse",Payload=json.dumps(payload))     
      
        
    return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
    }
