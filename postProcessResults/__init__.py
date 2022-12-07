import logging
import requests
import json
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
    downloaded_string = downloaded_bytes.decode('utf-8')
    downloaded_json = json.loads(downloaded_string)

    return downloaded_json

def writeFile(container, filepath, filename, data_in):

    file_system_client = service_client.get_file_system_client(file_system=container)
    directory_client = file_system_client.get_directory_client(filepath)

    file_client = directory_client.create_file(filename)
   
    file_client.upload_data(data=data_in,overwrite=True)

    return func.HttpResponse(body="Successfully saved JSON",status_code=200)


def processFile(container, filepath, filename, out_container,out_path,out_file):

    my_data = getFile(container, filepath, filename)
    invoiceTotal = my_data['analyzeResult']['documents'][0]['fields']['InvoiceTotal']['content']
    invoiceDate = my_data['analyzeResult']['documents'][0]['fields']['InvoiceDate']['content']
    vendorAddressRecipient = my_data['analyzeResult']['documents'][0]['fields']['VendorAddressRecipient']['content']
    customerAddressRecipient = my_data['analyzeResult']['documents'][0]['fields']['CustomerAddressRecipient']['content']

    print(customerAddressRecipient, invoiceDate, invoiceTotal, vendorAddressRecipient)

    result_string = '{"recipient": "'+customerAddressRecipient+'", "invoiceDate": "'+invoiceDate+'", "invoiceTotal": "'+invoiceTotal+'", "vendor": "'+vendorAddressRecipient+'" }'

    return writeFile(out_container,out_path,out_file,result_string)



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

    out_account = req_body.get('out_account')
    out_container = req_body.get('out_container')
    out_path = req_body.get('out_path')
    out_file = req_body.get('out_file')

    storagekey = req_body.get('storagekey')

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
    
    return processFile(in_container,in_path,in_file, out_container,out_path,out_file)
