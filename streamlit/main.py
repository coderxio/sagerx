import streamlit as st
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Dashboard Hub",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .app-info {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #667eea;
        margin: 1rem 0;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Main header
st.markdown("""
<div class="main-header">
    <h1>🏠 Welcome to Your Dashboard Hub</h1>
    <p>Your centralized collection of data apps and dashboards</p>
</div>
""", unsafe_allow_html=True)

# App descriptions
st.subheader("📊 Available Dashboards")
st.markdown("Use the sidebar to navigate between different applications:")

st.markdown("---")

# Instructions
st.subheader("💡 How to Use This Hub")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **🚀 Getting Started:**
    1. Look at the sidebar on the left
    2. Click on any app name to launch it
    3. Each app runs independently
    4. Return here anytime by clicking "Dashboard Hub"
    """)

with col2:
    st.markdown("""
    **➕ Adding New Apps:**
    1. Create a new `.py` file in the `pages/` folder
    2. Name it like: `2_🔍_Data_Explorer.py`
    3. It will automatically appear in the sidebar
    4. No need to modify this main page!
    """)

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #666; padding: 1rem;">
    <p>Dashboard Hub • Built with Streamlit • Last updated: {}</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d")), unsafe_allow_html=True)