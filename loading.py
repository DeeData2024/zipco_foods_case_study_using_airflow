from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import pandas as pd

def run_loading():
    data = pd.read_csv("cleaned_data.csv")
    products = pd.read_csv("products.csv")
    customers = pd.read_csv("customers.csv")
    staff = pd.read_csv("staff.csv")
    transactions = pd.read_csv("transactions.csv")

    #load the .env file to access the connection string
    load_dotenv()
    connection_string = os.getenv("MICROSOFT_AZURE_CONNECTION_VALUE")
    container_name = os.getenv("CONTAINER_NAME")
   
   #create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    #load data to azure blob storage
    files = [
        (data, 'rawdata/cleaned_data.csv'),
        (products, 'cleaneddata/products.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staff, 'cleaneddata/staff.csv'),
        (transactions, 'cleaneddata/transactions.csv')
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        file_data = file.to_csv(index=False)
        blob_client.upload_blob(file_data, overwrite=True)
        print(f"{blob_name} uploaded successfully!")