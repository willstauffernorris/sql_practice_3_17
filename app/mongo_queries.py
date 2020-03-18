# app/mongo_queries.py
#"How was working with MongoDB different from working with PostgreSQL? What was easier, and what was harder?"
## MongoDB was very difficult to connect to! I had to downgrade the version of Python I was using to make the certificates work with Mongo.

import pymongo
import os
from dotenv import load_dotenv
import json

load_dotenv()

DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri, ssl=True)
print("----------------")
print("CLIENT:", type(client), client)


db = client.test_database_will # "test_database" or whatever you want to call it
print("----------------")
print("DB:", type(db), db)



collection = db.pokemon_test # "pokemon_test" or whatever you want to call it
print("----------------")
print("COLLECTION:", type(collection), collection)



print("----------------")
print("COLLECTIONS:")
print(db.list_collection_names())

#This might be useful:
#json.dumps(my_dict)

import sqlite3

DB_FILEPATH = 'rpg_db.sqlite3'
connection = sqlite3.connect(DB_FILEPATH)
print('CONNECTION', connection)

'''
collection.insert_one({
    "name": "Pikachu",
    "level": 30,
    "exp": 76000000000,
    "hp": 400,
})
print("DOCS:", collection.count_documents({}))
print(collection.count_documents({"name": "Pikachu"}))
'''

### This is one way to go about it.
##pd.read_sql_query
'''
armory_json_string = json.dumps(armory_item.json)
'''

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "armory_item.json")
with open(DB_FILEPATH) as armory:
    json_data = json.load(armory)

print(json_data)

collection.insert_all({json_data})
    ###LIst of dctionaries
    ## key is the row values
    ## INsert many


print("DOCS:", collection.count_documents({}))
#print(collection.count_documents({"name": "Pikachu"}))


'''
insertion_query = "INSERT INTO passengers (survived, pclass, name, sex, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
execute_values(cursor, insertion_query, rows)
# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()
'''