import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import pandas
load_dotenv() # look in the .env file for env vars, and add them to the env
DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_HOST = os.getenv("DB_HOST", default="OOPS")
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
print("CONNECTION:", connection)
cursor = connection.cursor()
print("CURSOR:", cursor)
# GOAL: load the data/titanic.csv file into a PG database table!
# CREATE THE TABLE
# discern which columns (pandas?)
query = """
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
"""
cursor.execute(query)

cursor.execute("SELECT * from passengers;")
result = cursor.fetchall()
print("PASSENGERS:", len(result))

#How many passengers survived?
query2 = ('''
SELECT *
FROM passengers
WHERE survived=1;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print("SURVIVORS:", len(result2))

#How many passengers died?
query2 = ('''
SELECT *
FROM passengers
WHERE survived=0;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print("FATALITIES:", len(result2))

#How many passengers were in each class?
query2 = ('''
SELECT COUNT(pclass)
FROM passengers
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"PASSENGERS IN EACH CLASS: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")


#How many passengers survived within each class?

query2 = ('''
SELECT COUNT(pclass)
FROM passengers
WHERE survived=1
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"PASSENGERS SURVIVED IN EACH CLASS: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")


#How many passengers died within each class?

query2 = ('''
SELECT COUNT(pclass)
FROM passengers
WHERE survived=0
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"PASSENGERS DIED IN EACH CLASS: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")


#What was the average age of survivors?

query2 = ('''
SELECT AVG(age)
FROM passengers
WHERE survived=1
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"AVERAGE AGE OF SURVIVORS {result2}")

#What was the average age of nonsurvivors?

query2 = ('''
SELECT AVG(age)
FROM passengers
WHERE survived=0
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"AVERAGE AGE OF NON-SURVIVORS {result2}")

#What was the average age of each passenger class?

query2 = ('''
SELECT AVG(age)
FROM passengers
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"AVG AGE IN EACH CLASS: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")

#What was the average fare by passenger class?
query2 = ('''
SELECT AVG(fare)
FROM passengers
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"AVG FARE IN EACH CLASS: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")

#What was the average fare by survival?
query2 = ('''
SELECT AVG(fare)
FROM passengers
GROUP BY survived
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"AVG FARE BY SURVIVAL: DIED: {result2[0][0]} SURVIVED: {result2[1][0]}")

#How many siblings/spouses aboard on average, by passenger class?
query2 = ('''
SELECT AVG(sib_spouse_count)
FROM passengers
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"HOW MANY SIBLINGS ABOARD, ON AVERAGE: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")



#How many siblings/spouses aboard on average, by survival?
query2 = ('''
SELECT AVG(sib_spouse_count)
FROM passengers
GROUP BY survived
;
''')
cursor.execute(query2)
result2 = cursor.fetchall()
print(f"HOW MANY SIBLINGS ABOARD, ON AVERAGE: DIED: {result2[0][0]} SURVIVED: {result2[1][0]}")

#How many parents/children aboard on average, by passenger class?
query2 = ('''
SELECT AVG(parent_child_count)
FROM passengers
GROUP BY pclass
;
''')

cursor.execute(query2)
result2 = cursor.fetchall()
print(f"HOW MANY PARENTS/CHILDREN ABOARD, ON AVERAGE: FIRST: {result2[0][0]} SECOND: {result2[1][0]} THIRD: {result2[2][0]}")



#How many parents/children aboard on average, by survival?
query2 = ('''
SELECT AVG(parent_child_count)
FROM passengers
GROUP BY survived
;
''')
cursor.execute(query2)
result2 = cursor.fetchall()
print(f"HOW MANY PARENTS/CHILDREN ABOARD, ON AVERAGE: DIED: {result2[0][0]} SURVIVED: {result2[1][0]}")

#Do any passengers have the same name?
query2 = ('''
SELECT COUNT(DISTINCT name)
FROM passengers
;
''')
cursor.execute(query2)
result2 = cursor.fetchall()
print(f"NUMBER OF DISTINCT NAMES: {result2} (THIS IS THE SAME AS THE TOTAL PASSENGERS, NO SAME NAMES)")



##This is to insert the Titanic .csv into the table. Only runs once.
if len(result) == 0:
    # INSERT RECORDS
    #CSV_FILEPATH = "data/titanic.csv"
    CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "titanic.csv")
    print("FILE EXISTS?", os.path.isfile(CSV_FILEPATH))
    df = pandas.read_csv(CSV_FILEPATH)
    print(df.head())
    # rows should be a list of tuples
    insertion_query = "INSERT INTO passengers (survived, pclass, name, sex, age, sib_spouse_count, parent_child_count, fare) VALUES %s"
    execute_values(cursor, insertion_query, rows)
# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()
