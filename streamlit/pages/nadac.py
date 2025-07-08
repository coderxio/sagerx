import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import plotly.express as px
import random
from sagerx_db import run_query

# Set page config
st.set_page_config(
    page_title="NADAC Price Visualization",
    page_icon="💊",
    layout="wide"
)

# Initialize session state for random selection
if 'random_ingredient' not in st.session_state:
    st.session_state.random_ingredient = None

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

# Replace load_ingredient_options and load_price_data to use run_query
@st.cache_data
def load_ingredient_options() -> pd.DataFrame:
    query = '''
        SELECT DISTINCT ingredient_name
        FROM sagerx_dev.int_nadac_historical_pricing
        WHERE ingredient_name IS NOT NULL
        ORDER BY ingredient_name
    '''
    return run_query(query)

def load_price_data(selected_ingredient: str) -> pd.DataFrame:
    query = '''
        select
            *
        from sagerx_dev.int_nadac_historical_pricing nadac
        WHERE ingredient_name = %s
        AND start_date IS NOT NULL
        AND nadac_per_unit IS NOT NULL
        ORDER BY start_date
    '''
    df = run_query(query, params=[selected_ingredient])

    # Convert date column to datetime and ensure numeric types
    if not df.empty:
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['nadac_per_unit'] = df['nadac_per_unit'].astype(float)
        
    return df

def main():
    st.title("💊 NADAC Price Visualization")
    st.markdown("Search for a drug ingredient to view price trends")
    
    # Load ingredient options
    with st.spinner("Loading ingredient options..."):
        ingredient_df = load_ingredient_options()
    
    if ingredient_df.empty:
        st.error("Unable to load ingredient options. Please check database connection.")
        return
    
    # Search interface
    st.markdown("""
        <style>
        /* Align button with selectbox */
        div[data-testid="column"]:nth-of-type(2) {
            display: flex;
            align-items: flex-end;
        }
        div[data-testid="column"] > div.stButton > button {
            height: 42px;      /* Matches the height of the selectbox input */
        }
        </style>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([4, 1])
    
    # Random button first to ensure it updates before selectbox
    with col2:
        if st.button("🎲 Random", help="Select a random ingredient", use_container_width=True):
            st.session_state.random_ingredient = random.choice(ingredient_df['ingredient_name'].tolist())
    
    with col1:
        selected_ingredient = st.selectbox(
            "Search and select an ingredient:",
            options=ingredient_df['ingredient_name'].tolist(),
            index=None if st.session_state.random_ingredient is None else ingredient_df['ingredient_name'].tolist().index(st.session_state.random_ingredient),
            placeholder="Start typing to search..."
        )
        # Clear random selection after it's been used
        st.session_state.random_ingredient = None
    
    if not selected_ingredient:
        st.info("Please select an ingredient to view price trends.")
        return
    
    # Load price data
    with st.spinner("Loading price data..."):
        price_df = load_price_data(selected_ingredient)
    
    if price_df.empty:
        st.error(f"No price data available for {selected_ingredient}")
        return

    # Product filter
    st.markdown("---")
    st.markdown("### Filter Products")
    
    # Custom CSS to make multiselect pills wider
    st.markdown("""
        <style>
        /* Make multiselect pills take full width */
        .stMultiSelect [data-baseweb="tag"] {
            max-width: 100% !important;
            white-space: normal !important;
            height: auto !important;
            padding: 5px 10px !important;
            margin: 2px !important;
        }
        /* Ensure text inside pills doesn't get cut off */
        .stMultiSelect [data-baseweb="tag"] span {
            white-space: normal !important;
            overflow: visible !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Get unique products
    unique_products = sorted(price_df['product_name'].unique())
    
    selected_products = st.multiselect(
        "Select products to display (defaults to all):",
        options=unique_products,
        default=unique_products,
        help="You can remove products from the visualization by deselecting them here"
    )
    
    # Filter the dataframe based on selected products
    filtered_df = price_df[price_df['product_name'].isin(selected_products)]

    if filtered_df.empty:
        st.warning("No data to display for the selected products. Please select at least one product.")
        return

    # Create price trend chart
    fig = go.Figure()
    
    # Get unique products and assign colors using Plotly's default color sequence
    unique_products = sorted(filtered_df['product_name'].unique())
    colors = px.colors.qualitative.Set3[:len(unique_products)]  # Using Set3 color palette
    color_map = dict(zip(unique_products, colors))
    
    # Calculate average price per product per date
    avg_prices = filtered_df.groupby(['product_name', 'start_date', 'generic_brand_indicator'])['nadac_per_unit'].mean().reset_index()
    
    # Plot average price line for each product
    for product in unique_products:
        product_data = avg_prices[avg_prices['product_name'] == product]
        product_color = color_map[product]
        product_type = product_data['generic_brand_indicator'].iloc[0]
        
        # Get NDC count for this product for hover info
        ndc_count = len(filtered_df[filtered_df['product_name'] == product]['ndc'].unique())
        
        # Convert to lists to ensure proper plotting
        x_values = product_data['start_date'].tolist()
        y_values = product_data['nadac_per_unit'].tolist()
        
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=y_values,
                mode='lines+markers',
                name=f"{product} ({product_type})",
                line=dict(color=product_color),
                hovertemplate=(
                    "<b>Date:</b> %{x}<br>" +
                    "<b>Average Price:</b> $%{y:.4f}<br>" +
                    "<b>Product:</b> " + product + "<br>" +
                    "<b>Type:</b> " + product_type + "<br>" +
                    "<b>NDCs Averaged:</b> " + str(ndc_count) +
                    "<extra></extra>"
                )
            )
        )
    
    # Calculate y-axis range with padding
    y_min = avg_prices['nadac_per_unit'].min()
    y_max = avg_prices['nadac_per_unit'].max()
    y_padding = (y_max - y_min) * 0.1
    
    # Update layout
    fig.update_layout(
        title=f"Average NADAC Price Trends by Product for {selected_ingredient}",
        height=600,
        hovermode='closest',
        xaxis_title="Price Start Date",
        yaxis_title="Average NADAC Price per Unit (USD)",
        yaxis=dict(
            tickformat='$.4f',
            range=[y_min - y_padding, y_max + y_padding]
        ),
        showlegend=True,
        legend_title="Product (Type)"
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Display summary statistics
    st.subheader("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Selected Products", len(filtered_df['product_name'].unique()))
    
    with col2:
        st.metric("Total NDCs", len(filtered_df['ndc'].unique()))
    
    with col3:
        st.metric("Average NDCs per Product", f"{len(filtered_df['ndc'].unique()) / len(filtered_df['product_name'].unique()):.1f}")
    
    with col4:
        latest_date = filtered_df['start_date'].max()
        latest_avg = filtered_df[filtered_df['start_date'] == latest_date]['nadac_per_unit'].mean()
        st.metric("Latest Average Price", f"${latest_avg:.4f}")
    
    # Display raw data table with averages
    with st.expander("View Raw Data"):
        st.write("Average price data by product:")
        display_df = avg_prices.copy()
        display_df['nadac_per_unit_formatted'] = display_df['nadac_per_unit'].apply(lambda x: f"${x:.4f}")
        st.dataframe(
            display_df.sort_values(['generic_brand_indicator', 'product_name', 'start_date']),
            use_container_width=True
        )

if __name__ == "__main__":
    main()