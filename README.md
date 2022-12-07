# Invoice Processing

Azure functions to retrieve raw PDFs from an Azure storage account and process them using Azure Form Recognizer.
There are three steps to complete: 
- initialize the form recognizer and retrieve raw data
- get the extracted JSON information from Azure Form Recognizer and push to a storage account
- post-process the extracted information to only include necessary key-value pairs. 
