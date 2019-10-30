import sqlite3
import pandas as pd
from pandas import DataFrame

conn = sqlite3.connect('TableNames.db')

c = conn.cursor()

# c.execute("""CREATE TABLE clients (
#     [generated_id] INTEGER PRIMARY KEY,
#     [Client_Name] text,
#     [Country_ID] integer,
#     [Date] date)""")
#
# c.execute("""CREATE TABLE country (
#     [generated_id] INTEGER PRIMARY KEY,
#     [Country_ID] integer,
#     [Country_Name] text)""")

#
# c.execute("""CREATE TABLE daily_status (
#     [Client_Name] text,
#     [Country_Name] text,
#     [Date] date)""")

read_clients = pd.read_csv(r'Client_14-MAR-2019.csv')
read_clients.to_sql('CLIENTS', conn, if_exists='replace', index = False)

read_country = pd.read_csv(r'Country_14-MAR-2019.csv')
read_country.to_sql('COUNTRY', conn, if_exists='replace', index = False)

c.execute("""INSERT INTO daily_status (Client_Name, Country_Name, Date)
    SELECT DISTINCT clt.Client_Name,ctr.Country_Name, clt.Date
    FROM clients clt
    LEFT JOIN country ctr ON clt.Country_ID = ctr.Country_ID""")

c.eecute("""SELECT DISTINCT *
    FROM daily_status
    WHERE data = (SELECT max(Date) FROM daily_status)""")

#print(c.fetchall())

df = DataFrame(c.fetchall(), columns =['Client_Name', 'Date'])
#print(df)

df.to_sql('daily_status', conn, if_exists='append', index = False)

#export_csv = df.to_csv(r'export_list.csv', index = None, header = True)

conn.commit()
