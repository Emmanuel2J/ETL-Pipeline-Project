import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

#For extraction
def extract(url, table_attri):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns = table_attri)
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")
    for row in rows:
        col = row.find_all("td")
        if (len(col) != 0):
            data_dict = {
                "Name": col[1].contents[2],
                "MC_USD_Billion": str(col[2].contents[0]).replace("\n","")
            }
            df2 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df,df2], ignore_index = True)
    return df

#For Transformation
def transform(df):
    ex_df = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")
    exchange_rate = ex_df.set_index("Currency").to_dict()["Rate"]
    df["MC_EUR_Billion"] = [np.round(float(x)*exchange_rate["EUR"],2) for x in df["MC_USD_Billion"]]
    df["MC_GBP_Billion"] = [np.round(float(x)*exchange_rate["GBP"],2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(float(x)*exchange_rate["INR"],2) for x in df["MC_USD_Billion"]]

    return df


#For CSV Loading
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

#For Database Loading
def load_to_db(df,conn, table_name):
    df.to_sql(table_name, conn, if_exists = "replace", index = False)

#For Running Queries
def run_query(conn,query):
    query_output = pd.read_sql(query,conn)
    print(query_output)


#For Logging
def log_message(message):
    timestamp_format = "%Y-%m-%d - %H-%M-%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + "  " + message + "\n")


log_message("Intialization Of Values")
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attri = ["Name", "MC_USD_Billion"]
table_name = "Largest_banks" 
db_name = "Banks.db"
csv_path = "./Largest_banks_data.csv"  
db_name = "Banks.db"
log_message("Intialization Completed")


log_message("ETL Begins")

log_message("Extraction Begins")
df = extract(url,table_attri)
log_message("Extraction Ended")


log_message("Tranformation Begins")
df = transform(df)
log_message("Tranformation Ended")

log_message("Loading Phase Begun")

log_message("Loading to CSV")
load_to_csv(df, csv_path)
log_message("Loaded to CSV")

log_message("Connection Intialized")
conn = sqlite3.connect(db_name)

log_message("Loaded Dataframe to database")
load_to_db(df, conn, table_name)


query = "SELECT * FROM Largest_banks"

log_message("Running all the contents query")
run_query(conn,query)

query = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"

log_message("Running average market capatilization of all the banks in Billion GBP")
run_query(conn, query)

query = "SELECT Name from Largest_banks LIMIT 5"

log_message("Running only names of the top 5 banks query")
run_query(conn,query)

log_message("Terminating Connection")
conn.close()
log_message("SQL Connection Terminated")
log_message("ETL Process Ended")