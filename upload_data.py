from pymongo.mongo_client import MongoClient

import pandas as pd
import json

url="mongodb+srv://sonu:91420@cluster0.8qmooqr.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(url)

DATABASE_NAME="pwskills"
COLLECTION_NAME="waterfault"

df = pd.read_excel("wafer_dataset.xlsx")

json_record = json.loads(df.T.to_json()).values()
json_record = list(json_record)

client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)