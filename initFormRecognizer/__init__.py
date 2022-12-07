import logging
import requests
import azure.functions as func
from azure.identity import ChainedTokenCredential,ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient

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

def getFile(container, filepath, filename):

    file_system_client = service_client.get_file_system_client(file_system=container)
    directory_client = file_system_client.get_directory_client(filepath)
    file_client = directory_client.get_file_client(filename)

    download = file_client.download_file()
    downloaded_bytes = download.readall()

    return downloaded_bytes

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
             "Could not read body",
             status_code=500
        )
    in_account = req_body.get('in_account')
    in_container = req_body.get('in_container')
    in_path = req_body.get('in_path')
    in_file = req_body.get('in_file')

    storagekey = req_body.get('storagekey')

    #The model must be chanhged from prebuilt-document to prebuilt-id as stated here https://learn.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/get-started-v3-sdk-rest-api?pivots=programming-language-rest-api
    form_endpoint = req_body.get('form_endpoint')
    form_key = req_body.get('form_key')

    try:

        
        if not storagekey:
            initialize_storage_account(in_account)
        else:
            initialize_storage_account_local(in_account,storagekey)
    except Exception as e:
        return func.HttpResponse(
             "Could not initialize DataLakeFileSystemClient. Make sure that you passed the key in local development and if running as Azure Function, MSI is activated and has permissions.",
             status_code=500
        )
    #The model must be chanhged from prebuilt-document to prebuilt-id as stated here https://learn.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/get-started-v3-sdk-rest-api?pivots=programming-language-rest-api
    url = form_endpoint


    payload=getFile(in_container,in_path,in_file)
    headers = {
    'Ocp-Apim-Subscription-Key': form_key,
    'Content-Type': 'application/pdf'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    checkURL = response.headers["Operation-Location"]
    return checkURL
