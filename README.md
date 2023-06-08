# Invoice Processing

Azure functions to retrieve raw PDFs from an Azure storage account and process them using Azure Form Recognizer.
There are three steps to complete: 
- [Initialize](initRecognition) the form recognizer and retrieve raw data.
- [Get](getFormRecognizerResults) the extracted JSON information from Azure Form Recognizer and push to a storage account.
- [Post-process](postProcessResults) the extracted information to only include necessary key-value pairs.

Additionally, we have a [Jupyter notebook](AzureML/make_predictions_pipeline_template) which references a trained AzureML model to inference predictions on the extracted PDFs. The notebook takes a CSV file from your Azure storage account and runs the machine learning model on it, returning the results back to the storage account. This can be run as a pipeline from Azure Data Factory. 
