import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

# Configure Streamlit page
st.set_page_config(
    page_title="My App Dashboard",
    page_icon="📊",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_connection():
    """Create database connection"""
    database_url = 'postgresql://sagerx:sagerx@172.17.0.1:5432/sagerx'
    engine = create_engine(database_url)
    return engine

def main():
    st.title("📊 My App Dashboard")
    st.markdown("---")
    
    try:
        # Get database connection
        engine = get_connection()
        
        # Test connection
        with engine.connect() as conn:
            st.success("✅ Connected to PostgreSQL database")
            
            # Example query - adjust based on your database schema
            st.subheader("Database Overview")
            
            # Get table list
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'sagerx_dev'
            ORDER BY table_name;
            """
            
            tables_df = pd.read_sql(tables_query, engine)
            
            if not tables_df.empty:
                st.markdown("**Available Tables:**")
                for table in tables_df['table_name']:
                    st.markdown(f"• {table}")
                
                # Sample data display
                selected_table = st.selectbox("Select a table to preview:", tables_df['table_name'])
                
                if selected_table:
                    preview_query = f"SELECT * FROM sagerx_dev.{selected_table} LIMIT 10;"
                    try:
                        preview_df = pd.read_sql(preview_query, engine)
                        st.subheader(f"Preview of {selected_table}")
                        st.dataframe(preview_df)
                        
                        # Basic stats
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Rows in preview", len(preview_df))
                        with col2:
                            st.metric("Columns", len(preview_df.columns))
                            
                    except Exception as e:
                        st.error(f"Error previewing table: {str(e)}")
            else:
                st.info("No tables found in the database")
                
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        st.info("Make sure your PostgreSQL container is running and accessible")

if __name__ == "__main__":
    main()
