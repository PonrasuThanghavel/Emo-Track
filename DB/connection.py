import os
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Load variables from .env file
load_dotenv()

def load_to_database():
    try:
        client=MongoClient(os.getenv('connnection_string'))
        db=client[os.getenv('BB_NAME')]
        collection=db[os.getenv('COLLECTION_NAME')]
        print("connection successful")
    except ConnectionFailure as e:
        print("connection failure :{e}")
def retrive_data_from_database():
    try:
        # Create a MongoClient
        with MongoClient(os.getenv('connection_string')) as client:
            db = client[os.getenv('DB_NAME')]
            collection = db[os.getenv('COLLECTION_NAME')]
            data_list = list(collection.find())
            print("Data retrieval successful!")
            return data_list
    except ConnectionFailure as e:
        print(f"Error connecting to the database: {e}")

def save_csvfile(filename, data):
    df = pd.DataFrame(list(data))
    df.to_csv(filename, index=False)
    print("Data saved")

save_csvfile("../dataset/outputs.csv",retrive_data_from_database())
