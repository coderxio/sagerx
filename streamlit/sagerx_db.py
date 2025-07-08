import psycopg2
import pandas as pd
import streamlit as st

DATABASE_CONFIG = {
    'host': '172.17.0.1',
    'database': 'sagerx',
    'user': 'sagerx',
    'password': 'sagerx',
    'port': 5432,
    'schema': 'sagerx_dev'
}

def get_database_connection():
    try:
        conn = psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            database=DATABASE_CONFIG['database'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            port=DATABASE_CONFIG['port']
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

@st.cache_data
def run_query(query, params=None):
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error running query: {str(e)}")
        return pd.DataFrame() 