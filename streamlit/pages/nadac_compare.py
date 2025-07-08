import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import re
from sagerx_db import run_query

# Page configuration
st.set_page_config(
    page_title="Pharmacy Invoice Analyzer",
    page_icon="💊",
    layout="wide"
)

# Initialize session state
if 'benchmark_data' not in st.session_state:
    query = '''
        SELECT 
            *
        FROM sagerx_dev.int_nadac_pricing
        WHERE nadac_per_unit IS NOT NULL
    '''
    st.session_state.benchmark_data = run_query(query)

def clean_ndc(ndc):
    """Clean and standardize NDC numbers"""
    if pd.isna(ndc):
        return None
    ndc = str(ndc).strip()
    # Remove common prefixes and standardize format
    ndc = re.sub(r'^NDC[:\s]*', '', ndc, flags=re.IGNORECASE)
    ndc = re.sub(r'[^\d\-]', '', ndc)
    return ndc

def parse_invoice_data(df):
    """Parse and clean invoice data"""
    # Try to identify columns automatically
    column_mapping = {}
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['ndc', 'national drug code']):
            column_mapping['ndc_number'] = col
        elif any(keyword in col_lower for keyword in ['drug', 'product', 'item', 'name']):
            column_mapping['drug_name'] = col
        elif any(keyword in col_lower for keyword in ['qty', 'quantity', 'count']):
            column_mapping['quantity'] = col
        elif any(keyword in col_lower for keyword in ['unit cost', 'unit price', 'price per unit']):
            column_mapping['unit_cost'] = col
        elif any(keyword in col_lower for keyword in ['total', 'extended', 'amount']):
            column_mapping['total_cost'] = col
    
    # If we couldn't auto-detect, ask user to map columns
    if len(column_mapping) < 3:
        st.warning("Could not automatically detect all columns. Please map them manually.")
        return None, column_mapping
    
    # Create standardized dataframe
    invoice_df = pd.DataFrame()
    
    for standard_col, original_col in column_mapping.items():
        if original_col in df.columns:
            invoice_df[standard_col] = df[original_col]
    
    # Clean NDC numbers
    if 'ndc_number' in invoice_df.columns:
        invoice_df['ndc_number'] = invoice_df['ndc_number'].apply(clean_ndc)
    
    # Convert numeric columns
    numeric_cols = ['quantity', 'unit_cost', 'total_cost']
    for col in numeric_cols:
        if col in invoice_df.columns:
            invoice_df[col] = pd.to_numeric(invoice_df[col], errors='coerce')
    
    return invoice_df, column_mapping

def compare_prices(invoice_df, benchmark_df, threshold_percent=20):
    """Compare invoice prices against benchmark data"""
    results = []
    
    for idx, row in invoice_df.iterrows():
        # Try to match by NDC first
        benchmark_match = None
        
        if pd.notna(row.get('ndc_number')):
            benchmark_match = benchmark_df[benchmark_df['ndc_number'] == row['ndc_number']]
        
        # If no NDC match, try drug name matching
        if benchmark_match is None or benchmark_match.empty:
            if pd.notna(row.get('drug_name')):
                drug_name = str(row['drug_name']).lower()
                benchmark_match = benchmark_df[
                    benchmark_df['drug_name'].str.lower().str.contains(drug_name.split()[0], na=False)
                ]
        
        if not benchmark_match.empty:
            benchmark_price = benchmark_match.iloc[0]['benchmark_price']
            unit_cost = row.get('unit_cost', 0)
            
            if unit_cost > 0:
                price_difference = unit_cost - benchmark_price
                percent_difference = (price_difference / benchmark_price) * 100
                
                is_flagged = percent_difference > threshold_percent
                
                results.append({
                    'drug_name': row.get('drug_name', ''),
                    'ndc_number': row.get('ndc_number', ''),
                    'quantity': row.get('quantity', 0),
                    'unit_cost': unit_cost,
                    'benchmark_price': benchmark_price,
                    'price_difference': price_difference,
                    'percent_difference': percent_difference,
                    'total_overpay': price_difference * row.get('quantity', 0) if is_flagged else 0,
                    'is_flagged': is_flagged
                })
    
    return pd.DataFrame(results)

def create_sample_invoice():
    """Create a sample invoice file for testing"""
    sample_data = {
        'NDC Number': ['0069-2587-68', '0173-0687-55', '0378-3915-93', '0093-0058-01', '0781-5092-01'],
        'Drug Name': ['Lipitor 20mg Tablets', 'Metformin 500mg Tablets', 'Lisinopril 10mg Tablets', 
                     'Amlodipine 5mg Tablets', 'Atorvastatin 20mg Generic'],
        'Quantity': [30, 60, 90, 30, 30],
        'Unit Cost': [1.85, 0.25, 0.15, 0.18, 1.45],  # Some higher than benchmark
        'Total Cost': [55.50, 15.00, 13.50, 5.40, 43.50]
    }
    return pd.DataFrame(sample_data)

# --- START: Custom File Upload and Validation Logic ---

def normalize_column_name(col):
    """Normalize column names: lowercase, remove extra spaces, strip special chars."""
    return re.sub(r'\s+', ' ', col.strip().lower())

def find_column(df, candidates):
    """Find a column in df matching any of the candidate names (case/space-insensitive)."""
    norm_cols = {normalize_column_name(c): c for c in df.columns}
    for cand in candidates:
        norm_cand = normalize_column_name(cand)
        for norm_col, orig_col in norm_cols.items():
            if norm_cand == norm_col:
                return orig_col
    return None

def validate_and_parse_uploaded_file(df):
    # Find required columns
    ndc_col = find_column(df, ["NDC/UPC"])
    cost_col = find_column(df, ["Cost"])
    ship_size_col = find_column(df, ["Ship Size"])
    
    if not ndc_col or not cost_col or not ship_size_col:
        missing = [name for name, col in zip(["NDC/UPC", "Cost", "Ship Size"], [ndc_col, cost_col, ship_size_col]) if not col]
        return None, [f"Missing required columns: {', '.join(missing)}"]

    # Prepare output DataFrame
    out = pd.DataFrame()
    out['ndc'] = df[ndc_col].astype(str).str.strip()
    out['cost'] = pd.to_numeric(df[cost_col], errors='coerce')
    out['ship_size'] = df[ship_size_col].astype(str).str.strip()
    out['pack_qty'] = np.nan
    out['unit_size'] = np.nan
    out['unit_type'] = None
    out['unit_cost'] = np.nan

    # Validation regex for Ship Size
    ship_size_re = re.compile(r'^\((\d+)\)\s*([\d\.]+)\s*(EA|ML|GM)$', re.IGNORECASE)

    valid_rows = []
    for idx, row in out.iterrows():
        # Validate NDC/UPC
        ndc = row['ndc']
        if not ndc.isdigit():
            continue
        if len(ndc) != 11:
            if len(ndc) < 11:
                ndc = ndc.zfill(11)
            else:
                continue
        # Validate Cost
        cost = row['cost']
        if pd.isna(cost):
            continue
        # Validate Ship Size
        ship_size = row['ship_size']
        m = ship_size_re.match(ship_size)
        if not m:
            continue
        pack_qty, unit_size, unit_type = m.groups()
        pack_qty = int(pack_qty)
        unit_size = float(unit_size)
        unit_type = unit_type.upper()
        unit_cost = cost / (pack_qty * unit_size) if (pack_qty * unit_size) != 0 else np.nan
        valid_rows.append({
            'ndc': ndc,
            'cost': cost,
            'ship_size': ship_size,
            'pack_qty': pack_qty,
            'unit_size': unit_size,
            'unit_type': unit_type,
            'unit_cost': unit_cost
        })
    if not valid_rows:
        return None, ["No valid rows found in the uploaded file."]
    out = pd.DataFrame(valid_rows)
    out = out[['ndc', 'cost', 'ship_size', 'pack_qty', 'unit_size', 'unit_cost', 'unit_type']]
    return out, None

# --- END: Custom File Upload and Validation Logic ---

# Main app layout
st.title("💊 Pharmacy Invoice Analyzer")
st.markdown("Upload your medication purchasing invoices to identify potential overpayments compared to benchmark prices.")

# Sidebar for settings
st.sidebar.header("Settings")
threshold = st.sidebar.slider("Price Difference Threshold (%)", 5, 50, 20)
st.sidebar.markdown("Items exceeding this percentage above benchmark will be flagged.")

# Create tabs
tab1, tab2, tab3 = st.tabs(["📤 Upload Invoice", "📊 Benchmark Data", "📋 Results"])

with tab1:
    st.header("Upload Invoice Data")
    
    # Option to download sample file
    if st.button("Download Sample Invoice Format"):
        sample_df = create_sample_invoice()
        csv = sample_df.to_csv(index=False)
        st.download_button(
            label="Download Sample CSV",
            data=csv,
            file_name="sample_invoice.csv",
            mime="text/csv"
        )
    
    # File upload with 10MB limit
    uploaded_file = st.file_uploader(
        "Choose invoice file",
        type=['csv', 'xlsx', 'xls'],
        help="Upload CSV or Excel file containing invoice data (max 10MB)"
    )
    
    if uploaded_file is not None:
        if uploaded_file.size > 10 * 1024 * 1024:
            st.error("File is too large. Maximum allowed size is 10MB.")
        else:
            try:
                # Read file
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                # Validate and parse
                parsed_df, errors = validate_and_parse_uploaded_file(df)
                st.success(f"File uploaded successfully! Found {len(df)} rows. Validated {len(parsed_df)} rows.")
                if errors:
                    st.error("\n".join(errors))
                else:
                    st.subheader("Validated Data Preview")
                    st.dataframe(parsed_df)
                    # --- JOIN with benchmark data and compare ---
                    bench = st.session_state.benchmark_data
                    # Ensure NDC columns are string and 11 digits
                    bench = bench.copy()
                    bench['ndc'] = bench['ndc'].astype(str).str.zfill(11)
                    merged = pd.merge(parsed_df, bench, on='ndc', how='left', suffixes=('', '_benchmark'))
                    # Compare unit_cost to nadac_per_unit
                    merged['nadac_per_unit'] = pd.to_numeric(merged['nadac_per_unit'], errors='coerce')
                    merged['unit_cost_vs_nadac'] = merged['unit_cost'] - merged['nadac_per_unit']
                    merged['is_over_nadac'] = merged['unit_cost'] > merged['nadac_per_unit']
                    st.session_state.upload_results = merged
                    st.session_state.show_results_tab = True
            except Exception as e:
                st.error(f"Error reading file: {e}")

with tab2:
    st.header("Benchmark Price Data")
    st.markdown("Current benchmark prices in the system:")
    
    # Display benchmark data
    st.dataframe(st.session_state.benchmark_data)
    
    # Option to add new benchmark data
    st.subheader("Add New Benchmark Entry")
    with st.form("add_benchmark"):
        col1, col2 = st.columns(2)
        with col1:
            new_ndc = st.text_input("NDC Number")
            new_drug = st.text_input("Drug Name")
            new_generic = st.text_input("Generic Name")
        with col2:
            new_strength = st.text_input("Strength")
            new_price = st.number_input("Benchmark Price", min_value=0.0, step=0.01)
            new_source = st.text_input("Source", value="Manual")
        
        if st.form_submit_button("Add Benchmark"):
            new_row = pd.DataFrame({
                'ndc_number': [new_ndc],
                'drug_name': [new_drug],
                'generic_name': [new_generic],
                'strength': [new_strength],
                'benchmark_price': [new_price],
                'source': [new_source]
            })
            st.session_state.benchmark_data = pd.concat([st.session_state.benchmark_data, new_row], ignore_index=True)
            st.success("Benchmark entry added!")
            st.rerun()

with tab3:
    st.header("Analysis Results")
    # If user just uploaded and validated, show results immediately
    if st.session_state.get('show_results_tab'):
        st.session_state.show_results_tab = False  # Reset flag
        if 'upload_results' in st.session_state:
            st.subheader("Upload vs. Benchmark Comparison")
            st.dataframe(st.session_state.upload_results)
    
    if 'comparison_results' in st.session_state:
        results = st.session_state.comparison_results
        
        if not results.empty:
            # Filter options
            show_all = st.checkbox("Show all items", value=False)
            
            if show_all:
                display_results = results
            else:
                display_results = results[results['is_flagged']]
            
            if not display_results.empty:
                # Format display dataframe
                display_df = display_results.copy()
                display_df['unit_cost'] = display_df['unit_cost'].apply(lambda x: f"${x:.2f}")
                display_df['benchmark_price'] = display_df['benchmark_price'].apply(lambda x: f"${x:.2f}")
                display_df['price_difference'] = display_df['price_difference'].apply(lambda x: f"${x:.2f}")
                display_df['percent_difference'] = display_df['percent_difference'].apply(lambda x: f"{x:.1f}%")
                display_df['total_overpay'] = display_df['total_overpay'].apply(lambda x: f"${x:.2f}")
                
                # Sort by highest overpay
                display_df = display_df.sort_values('total_overpay', ascending=False)
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Export functionality
                csv = results.to_csv(index=False)
                st.download_button(
                    label="Download Full Results",
                    data=csv,
                    file_name=f"price_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No items flagged with current threshold. Try lowering the threshold or upload a different invoice.")
        else:
            st.info("No comparison results available.")
    else:
        st.info("Upload an invoice file to see analysis results.")

# Footer
st.markdown("---")
st.markdown("💡 **Tip**: Start with the sample invoice format to test the system, then upload your actual invoice files.")