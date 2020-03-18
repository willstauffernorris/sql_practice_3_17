import psycopg2
import os
from dotenv import load_dotenv

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
CREATE TABLE IF NOT EXISTS test_table (
  id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  data JSONB
);
'''

cursor.execute(query)

cursor.execute('SELECT * from test_table;')
result = cursor.fetchall()
print("RESULT:", len(result))


insertion_query = """
INSERT INTO test_table (name, data) VALUES
(
  'A row name',
  null
),
(
  'Another row, with JSON',
  '{ "a": 1, "b": ["dog", "cat", 42], "c": true }'::JSONB
);
"""

cursor.execute(insertion_query)

cursor.execute('SELECT * from test_table;')
result = cursor.fetchall()
print("RESULT:", len(result))


##Save the transactions
#connection.commit()
