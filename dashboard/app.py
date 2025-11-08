import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Music Analytics Dashboard",
    layout="wide"
)

# Navigation setup
pg = st.navigation([
    st.Page("main_page.py", title="Home"),
    st.Page("test.py", title="Track"),
])

# Run the selected page
pg.run()