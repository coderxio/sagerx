import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from typing import List, Tuple, Optional
import re

# Set page config
st.set_page_config(
    page_title="NADAC Price Visualization",
    page_icon="💊",
    layout="wide"
)

# Database connection configuration
# Update these parameters based on your database setup
DATABASE_CONFIG = {
    'host': '172.17.0.1',
    'database': 'sagerx',
    'user': 'sagerx',
    'password': 'sagerx',
    'port': 5432,  # Default PostgreSQL port
    'schema': 'sagerx_dev'  # Add schema configuration
}

@st.cache_resource
def get_database_connection():
    """
    Create database connection. Update this function based on your database type.
    Examples for different database types are provided below.
    """
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
def load_typeahead_data() -> pd.DataFrame:
    """Load NDC and description data for typeahead functionality."""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT DISTINCT 
            ndc,
            ndc_description
        FROM {}.stg_nadac__enhanced_nadac
        WHERE ndc IS NOT NULL 
        AND ndc_description IS NOT NULL
        ORDER BY ndc_description
        """.format(DATABASE_CONFIG['schema'])
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading typeahead data: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def load_price_data(selected_ndc: str) -> pd.DataFrame:
    """Load price data for the selected NDC."""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            price_start_date,
            nadac_per_unit,
            ndc,
            ndc_description
        FROM {}.stg_nadac__enhanced_nadac
        WHERE ndc = %s
        AND price_start_date IS NOT NULL
        AND nadac_per_unit IS NOT NULL
        ORDER BY price_start_date
        """.format(DATABASE_CONFIG['schema'])
        
        df = pd.read_sql_query(query, conn, params=[selected_ndc])
        conn.close()
        
        # Convert date column to datetime if it's not already
        if not df.empty:
            df['price_start_date'] = pd.to_datetime(df['price_start_date'])
            
        return df
    except Exception as e:
        st.error(f"Error loading price data: {str(e)}")
        return pd.DataFrame()

def create_search_options(df: pd.DataFrame) -> List[str]:
    """Create searchable options combining NDC and description."""
    if df.empty:
        return []
    
    options = []
    for _, row in df.iterrows():
        option = f"{row['ndc']} - {row['ndc_description']}"
        options.append(option)
    
    return sorted(options)

def extract_ndc_from_selection(selection: str) -> str:
    """Extract NDC code from the selected option."""
    if not selection:
        return ""
    
    # Extract NDC code (everything before the first " - ")
    ndc = selection.split(" - ")[0]
    return ndc

def filter_options(options: List[str], search_term: str) -> List[str]:
    """Filter options based on search term."""
    if not search_term:
        return options[:50]  # Limit to first 50 items for performance
    
    search_term_lower = search_term.lower()
    filtered = [
        option for option in options 
        if search_term_lower in option.lower()
    ]
    
    return filtered[:50]  # Limit to 50 results

def main():
    st.title("💊 NADAC Price Visualization")
    st.markdown("Search for a drug by NDC code or description to view price trends over time.")
    
    # Load typeahead data
    with st.spinner("Loading drug data..."):
        typeahead_df = load_typeahead_data()
    
    if typeahead_df.empty:
        st.error("Unable to load drug data. Please check your database connection.")
        return
    
    # Create search options
    all_options = create_search_options(typeahead_df)
    
    # Search interface
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            "Search by NDC code or drug description:",
            placeholder="Type to search for a drug...",
            help="Start typing to search for drugs by NDC code or description"
        )
    
    # Filter options based on search term
    filtered_options = filter_options(all_options, search_term)
    
    if search_term and filtered_options:
        with col1:
            selected_option = st.selectbox(
                "Select a drug:",
                options=filtered_options,
                index=None,
                placeholder="Choose from the filtered results..."
            )
    elif search_term and not filtered_options:
        st.warning("No drugs found matching your search. Try a different search term.")
        selected_option = None
    else:
        selected_option = None
    
    # Display results
    if selected_option:
        selected_ndc = extract_ndc_from_selection(selected_option)
        
        # Display selected drug info
        st.success(f"Selected: {selected_option}")
        
        # Load price data
        with st.spinner("Loading price data..."):
            price_df = load_price_data(selected_ndc)
        
        if price_df.empty:
            st.warning("No price data available for the selected drug.")
        else:
            # Display summary statistics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Data Points", len(price_df))
            
            with col2:
                avg_price = price_df['nadac_per_unit'].mean()
                st.metric("Average Price", f"${avg_price:.4f}")
            
            with col3:
                min_price = price_df['nadac_per_unit'].min()
                st.metric("Minimum Price", f"${min_price:.4f}")
            
            with col4:
                max_price = price_df['nadac_per_unit'].max()
                st.metric("Maximum Price", f"${max_price:.4f}")
            
            # Create price trend chart
            st.subheader("Price Trend Over Time")
            
            fig = px.line(
                price_df, 
                x='price_start_date', 
                y='nadac_per_unit',
                title=f"NADAC Price Trend: {price_df.iloc[0]['ndc_description']}",
                labels={
                    'price_start_date': 'Price Start Date',
                    'nadac_per_unit': 'NADAC Price per Unit (USD)',
                },
                markers=True
            )
            
            # Customize the chart
            fig.update_layout(
                height=500,
                hovermode='x unified',
                xaxis_title="Price Start Date",
                yaxis_title="NADAC Price per Unit (USD)",
                yaxis_tickformat='$.4f'
            )
            
            fig.update_traces(
                hovertemplate='<b>Date:</b> %{x}<br><b>Price:</b> $%{y:.4f}<extra></extra>'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display raw data table
            with st.expander("View Raw Data"):
                st.dataframe(
                    price_df[['price_start_date', 'nadac_per_unit']].round(4),
                    use_container_width=True
                )
    
    # Instructions for database setup
    with st.expander("Database Setup Instructions"):
        st.markdown("""
        **To connect to your database, update the following in the code:**
        
        1. **Install required database driver:**
           - For PostgreSQL: `pip install psycopg2-binary`
           - For MySQL: `pip install mysql-connector-python`
           - For SQL Server: `pip install pyodbc`
        
        2. **Update DATABASE_CONFIG dictionary** with your connection details
        
        3. **Uncomment the appropriate database connection code** in the `get_database_connection()` function
        
        4. **Ensure your table `stg_nadac__enhanced_nadac` has the required columns:**
           - `ndc` (NDC code)
           - `ndc_description` (drug description)
           - `price_start_date` (date)
           - `nadac_per_unit` (price per unit)
        """)

if __name__ == "__main__":
    main()