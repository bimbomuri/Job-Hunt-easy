import pandas as pd
import psycopg2
import json

df = pd.read_csv('Data/updated_organization_file.csv')
df = df.rename(columns={"Organisation Name": "Organisation_Name",
    "Town/City": "Town_City",
    "Type & Rating": "Type_Rating",
    "Industry": "industry"})

# Clean the industry column by removing leading and trailing whitespace and newlines
df['industry'] = df['industry'].str.strip()
#print(df.columns)
values = df.values.tolist()

with open('config/Configuration.json') as config_file:
   data = json.load(config_file)

_host = data['host']
_user = data['user']
_password = data['password']
_database = data['database']

conn = psycopg2.connect(host = _host, user =_user, password=_password, database=_database)

insert_query = """
INSERT INTO UK_Licensed_Sponsor (
    Organisation_Name,Town_City,County,Type_Rating,Route,industry
) VALUES (%s,%s,%s,%s,%s,%s)
"""
cur = conn.cursor()

# Iterate over the DataFrame and insert each row into the database
for index, row in df.iterrows():
    cur.execute(insert_query, (
        row['Organisation_Name'],
        row['Town_City'],
        row['County'],
        row['Type_Rating'],
        row['Route'],
        row['industry']
    ))
    
conn.commit()

print('Ingestion completed!')
#result = cur.fetchall()
#print(df.columns)