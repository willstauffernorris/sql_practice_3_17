import psycopg2
import os
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values






load_dotenv()

DB_NAME=os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_HOST=os.getenv("DB_HOST")

connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASSWORD, host=DB_HOST)
print("CONNETION", connection)


cursor = connection.cursor()
print("CURSOR", cursor)

query = '''
CREATE TABLE IF NOT EXISTS passengers (
  id SERIAL PRIMARY KEY,
  survived bool,
  pclass int,
  name varchar,
  sex varchar,
  age int,
  sib_spouse_count int,
  parent_child_count int,
  fare float8

);
'''

cursor.execute(query)


cursor.execute('SELECT * from passengers;')
result = cursor.fetchall()
print("PASSENGERS:", len(result))

if len(result)==0:
    #insert


'''
import pandas as pd

df = pd.read_csv("titanic.csv")

print(df.head(5))

###Need to get all my data in a csv into a list of tuples

print(df.dtypes)
'''

##Save the transactions
connection.commit()