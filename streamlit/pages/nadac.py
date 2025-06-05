import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2

# Set page config
st.set_page_config(
    page_title="NADAC Price Visualization",
    page_icon="💊",
    layout="wide"
)

# Database connection configuration
DATABASE_CONFIG = {
    'host': '172.17.0.1',
    'database': 'sagerx',
    'user': 'sagerx',
    'password': 'sagerx',
    'port': 5432,
    'schema': 'sagerx_dev'
}

def get_database_connection():
    """Create database connection."""
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
def load_ingredient_options() -> pd.DataFrame:
    """Load all available ingredients."""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT DISTINCT ingredient_name
        FROM sagerx_dev.int_rxnorm_clinical_products_to_ingredients
        WHERE ingredient_name IS NOT NULL
        ORDER BY ingredient_name
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading ingredient options: {str(e)}")
        return pd.DataFrame()

def load_price_data(selected_ingredient: str) -> pd.DataFrame:
    """Load price data for all NDCs containing the selected ingredient."""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        select
            ing.ingredient_name,
            case
                when prod.product_tty in ('SCD', 'GPCK')
                    then 'Generic'
                when prod.product_tty in ('SBD', 'BPCK')
                    then 'Brand'
                else 'Unknown'
            end as generic_brand_indicator,
            nadac.ndc,
            nadac.ndc_description,
            nadac.price_start_date,
            nadac.nadac_per_unit
        from sagerx_dev.stg_nadac__enhanced_nadac nadac
        left join sagerx_dev.int_rxnorm_ndcs_to_products prod
            on prod.ndc = nadac.ndc
        left join sagerx_dev.int_rxnorm_clinical_products_to_ingredients ing
            on ing.clinical_product_rxcui = prod.clinical_product_rxcui
        WHERE ing.ingredient_name = %s
        AND nadac.price_start_date IS NOT NULL
        AND nadac.nadac_per_unit IS NOT NULL
        ORDER BY nadac.price_start_date
        """
        
        df = pd.read_sql_query(query, conn, params=[selected_ingredient])
        conn.close()
        
        # Convert date column to datetime and ensure numeric types
        if not df.empty:
            df['price_start_date'] = pd.to_datetime(df['price_start_date'])
            df['nadac_per_unit'] = df['nadac_per_unit'].astype(float)
            
        return df
    except Exception as e:
        st.error(f"Error loading price data: {str(e)}")
        return pd.DataFrame()

def main():
    st.title("💊 NADAC Price Visualization")
    st.markdown("Search for a drug ingredient to view price trends across all NDCs")
    
    # Load ingredient options
    with st.spinner("Loading ingredient options..."):
        ingredient_df = load_ingredient_options()
    
    if ingredient_df.empty:
        st.error("Unable to load ingredient options. Please check database connection.")
        return
    
    # Search interface
    selected_ingredient = st.selectbox(
        "Search and select an ingredient:",
        options=ingredient_df['ingredient_name'].tolist(),
        index=None,
        placeholder="Start typing to search..."
    )
    
    if not selected_ingredient:
        st.info("Please select an ingredient to view price trends.")
        return
    
    # Load price data
    with st.spinner("Loading price data..."):
        price_df = load_price_data(selected_ingredient)
    
    if price_df.empty:
        st.error(f"No price data available for {selected_ingredient}")
        return

    # Create price trend chart
    fig = go.Figure()
    
    # Plot lines for each NDC, colored by generic/brand status
    for ndc in price_df['ndc'].unique():
        ndc_data = price_df[price_df['ndc'] == ndc]
        color = '#1f77b4' if ndc_data['generic_brand_indicator'].iloc[0] == 'Generic' else '#ff7f0e'
        
        # Convert to lists to ensure proper plotting
        x_values = ndc_data['price_start_date'].tolist()
        y_values = ndc_data['nadac_per_unit'].tolist()
        
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode='lines+markers',
                name=f"{ndc} - {ndc_data['generic_brand_indicator'].iloc[0]}",
                line=dict(color=color),
                hovertemplate=(
                    "<b>Date:</b> %{x}<br>" +
                    "<b>Price:</b> $%{y:.4f}<br>" +
                    "<b>NDC:</b> " + ndc + "<br>" +
                    "<b>Type:</b> " + ndc_data['generic_brand_indicator'].iloc[0] +
                    "<extra></extra>"
                )
            )
        )
    
    # Calculate y-axis range with padding
    y_min = price_df['nadac_per_unit'].min()
    y_max = price_df['nadac_per_unit'].max()
    y_padding = (y_max - y_min) * 0.1
    
    # Update layout
    fig.update_layout(
        title=f"NADAC Price Trends for {selected_ingredient}",
        height=600,
        hovermode='x unified',
        xaxis_title="Price Start Date",
        yaxis_title="NADAC Price per Unit (USD)",
        yaxis=dict(
            tickformat='$.4f',
            range=[y_min - y_padding, y_max + y_padding]
        ),
        showlegend=True,
        legend_title="NDC - Type"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Display summary statistics
    st.subheader("Summary Statistics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total NDCs", len(price_df['ndc'].unique()))
    
    with col2:
        generic_count = len(price_df[price_df['generic_brand_indicator'] == 'Generic']['ndc'].unique())
        st.metric("Generic NDCs", generic_count)
    
    with col3:
        brand_count = len(price_df[price_df['generic_brand_indicator'] == 'Brand']['ndc'].unique())
        st.metric("Brand NDCs", brand_count)
    
    # Display raw data table
    with st.expander("View Raw Data"):
        st.write("Detailed price data:")
        display_df = price_df[['price_start_date', 'ndc', 'generic_brand_indicator', 'nadac_per_unit']].copy()
        display_df['nadac_per_unit_formatted'] = display_df['nadac_per_unit'].apply(lambda x: f"${x:.4f}")
        st.dataframe(
            display_df.sort_values(['generic_brand_indicator', 'ndc', 'price_start_date']),
            use_container_width=True
        )

if __name__ == "__main__":
    main()