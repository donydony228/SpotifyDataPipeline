import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Music Analytics Dashboard",
    layout="wide"
)

# Navigation setup
pg = st.navigation({
    "Analytics": [
    st.Page("main_page.py", title="Home"),
    st.Page("track.py", title="Track"),
    st.Page("artist.py", title="Artist"),
    st.Page("album.py", title="Album"),
    ],
    "Data Governance": [
    st.Page("data_quality.py", title="Data Quality"),
    # st.Page("data_dictionary.py", title="Data Dictionary"),
    # st.Page("data_dictionary_admin.py", title="Data Dictionary Admin"),
    ],
    "About": [  
    st.Page("about.py", title="About"),
    ],
})

# Run the selected page
pg.run()