import sqlite3
import pandas as pd

# dados de navegação do usuário
raw_data = pd.read_csv("data/raw_data.csv")
# convert to numeric:
raw_data["value"] = raw_data["value"].str.replace(',', '.').astype('float')
# convert to datetime:
raw_data["event_timestamp"] = pd.to_datetime(raw_data["event_timestamp"], format = "%d/%m/%Y %H:%M:%S")
raw_data["event_date"] = pd.to_datetime(raw_data["event_date"])

# dados das informações
transactions = pd.read_csv("data/transactions.csv")
# convert to numeric:
transactions["revenue"] = transactions["revenue"].str.replace(",", ".").astype("float")
# convert to datetime:
transactions["updated_at"] = pd.to_datetime(transactions["updated_at"], format = "%d/%m/%Y %H:%M:%S")
transactions["created_at"] = pd.to_datetime(transactions["created_at"], format = "%d/%m/%Y %H:%M:%S")

# reading info on spending:
media_spend = pd.read_csv("data/media_spend.csv")
# convert to datetime:
media_spend["data"] = pd.to_datetime(media_spend["data"])

# open connection to a SQLlite
conn = sqlite3.connect("data/Martech.db")

# insert the tables into the DB:
raw_data.to_sql("raw_data", conn, index=False, if_exists="replace")
transactions.to_sql("transactions", conn, index=False, if_exists="replace")
media_spend.to_sql("media_spend", conn, index=False, if_exists="replace")

# close connection
conn.close()

