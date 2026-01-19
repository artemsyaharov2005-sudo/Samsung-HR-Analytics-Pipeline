import pandas as pd
import streamlit as st
from pyhive import hive

st.set_page_config(page_title="IT vacancies dashboard (Hive)", layout="wide")
st.title("IT vacancies dashboard — reading from Hive")

@st.cache_data(ttl=60)
def read_hive(sql: str) -> pd.DataFrame:
    conn = hive.Connection(
        host="localhost",
        port=10000,
        username="root",
        database="samsung",
    )
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()

st.header("Top cities by vacancies")
df_top = read_hive("""
SELECT area_name, COUNT(*) AS vacancies
FROM it_vacancies_clean
GROUP BY area_name
ORDER BY vacancies DESC
LIMIT 20
""")
st.dataframe(df_top, use_container_width=True)

st.header("Salary by area (from mart table)")
df_salary = read_hive("""
SELECT area_name, vacancies_with_salary, avg_rub
FROM it_salary_by_area
ORDER BY vacancies_with_salary DESC
LIMIT 30
""")
st.dataframe(df_salary, use_container_width=True)

st.header("Charts")
st.bar_chart(df_top.set_index("area_name"))
