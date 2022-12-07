import logging
from time import sleep
import requests
import azure.functions as func
from azure.identity import ChainedTokenCredential,ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient
import json 

def initialize_storage_account(storage_account_name):
    
    try:
        global service_client
        MSI_credential = ManagedIdentityCredential()
    
        credential_chain = ChainedTokenCredential(MSI_credential)
        
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=credential_chain)
    
    except Exception as e:
        print(e)

def initialize_storage_account_local(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

def writeFile(container, filepath, filename, data_in):

    file_system_client = service_client.get_file_system_client(file_system=container)
    directory_client = file_system_client.get_directory_client(filepath)

    file_client = directory_client.create_file(filename)
   
    file_client.upload_data(data=data_in,overwrite=True)

    return func.HttpResponse(body="Successfully saved JSON",status_code=200)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
             "Could not read body",
             status_code=500
        )
    out_account = req_body.get('out_account')
    out_container = req_body.get('out_container')
    out_path = req_body.get('out_path')
    out_file = req_body.get('out_file')

    storagekey = req_body.get('storagekey')
    lookUpUrl = req_body.get('lookupurl')
    form_key = req_body.get('form_key')

    url = lookUpUrl
    try: 
        if not storagekey:
            initialize_storage_account(out_account)
        else:
            initialize_storage_account_local(out_account,storagekey)
    except Exception as e:
        return func.HttpResponse(
             "Could not initialize DataLakeFileSystemClient. Make sure that you passed the key in local development and if running as Azure Function, MSI is activated and has permissions.",
             status_code=500
        )

    payload={}
    headers = {
    'Ocp-Apim-Subscription-Key': form_key
    }

    sleep(5)

    response = requests.request("GET", url, headers=headers, data=payload)

    result_data = response.text

    data_json = json.loads(result_data)

    logging.info(result_data)

    logging.info(data_json['status'])

    if data_json['status'] == "succeeded":

        return writeFile(out_container,out_path,out_file,result_data)

    elif data_json['status'] == "failed":

        return func.HttpResponse(body=data_json['status'],status_code=500)
    else:
        return func.HttpResponse(body=data_json['status'],status_code=500)