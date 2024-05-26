import pandas as pd
import psycopg2
import json

with open('config/Configuration.json') as config_file:
   data = json.load(config_file)

_host = data['host']
_user = data['user']
_password = data['password']
_database = data['database']

conn = psycopg2.connect(host = _host, user =_user, password=_password, database=_database)

result = input('Enter industry: ').strip()
#print(result)
cur = conn.cursor()
if not result:
    print('No industry entered. Please enter a valid industry.')
else:
    select_query = """
    select organisation_Name from UK_Licensed_Sponsor where industry like %s
    """
    cur.execute(select_query, (f"%{result}%",))
    rows= cur.fetchall()
    
    if rows:
        for row in rows:
            print(row)
    else:
        print(f'No organizations found for industry: {result}')
    