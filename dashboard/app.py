import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Music Analytics Dashboard",
    layout="wide"
)

# Navigation setup
pg = st.navigation([
    st.Page("main_page.py", title="Home"),
    st.Page("track.py", title="Track"),
    st.Page("artist.py", title="Artist"),
    st.Page("album.py", title="Album"),
    st.Page("about.py", title="About"),
])

# Run the selected page
pg.run()