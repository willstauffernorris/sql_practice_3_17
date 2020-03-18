import psycopg2
import os
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values
import pandas as pd





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
  survived int,
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
    CSV_filepath = os.path.join(os.path.dirname(__file__), "..", "titanic.csv")
    df = pd.read_csv(CSV_filepath)

    print(df.head(5))
###Need to get all my data in a csv into a list of tuples
    rows = list(df.itertuples(index=False, name=None))


    insertion_query = "INSERT INTO passengers (survived, pclass, name, sex, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
    execute_values(cursor, insertion_query, rows)

#breakpoint()


#print(df.dtypes)


##Save the transactions
connection.commit()