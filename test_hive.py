from pyhive import hive
import pandas as pd

conn = hive.Connection(
    host="127.0.0.1",
    port=10000,
    username="root",
    database="samsung"
)

df = pd.read_sql("SHOW TABLES", conn)
print(df)
