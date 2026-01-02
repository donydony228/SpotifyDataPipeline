import streamlit as st
# import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.markdown("""
<style>
.block-container {
    padding-top: 0rem;
    padding-right: 2rem;
    padding-left: 2rem;
    padding-bottom: 0rem;
}

.st-emotion-cache-1cypcd9 {
    padding-top: 1rem;
}

.st-emotion-cache-1y4pm5c { 
    padding-top: 0rem; 
}
</style>
""", unsafe_allow_html=True)
# -----------------------------------------------

# Image Display
st.image("dashboard/IMG_2446.jpg", width='stretch')

st.title("Spotify Music Analytics Platform")
st.subheader("Transform Your Spotify Listening Data into Interesting Insights")

st.markdown("---")

# Project overview
st.header("What This Platform Does")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Automated Data Collection")
    st.write("""
    Automatically fetches your Spotify listening history daily using the Spotify Web API, 
    ensuring no listening session is missed. The system runs scheduled tasks through Apache 
    Airflow to maintain consistent data collection.
    """)
    
    st.subheader("Advanced Music Analysis")
    st.write("""
    Analyzes audio features like energy, valence, danceability, and acousticness to understand 
    your musical preferences and mood patterns over time. Creates detailed profiles of your 
    listening behavior across different contexts.
    """)

with col2:
    st.subheader("Interactive Visualizations")
    st.write("""
    Creates beautiful, interactive charts and heatmaps showing your listening patterns across 
    different times, artists, and genres. All visualizations are built with Plotly for 
    responsive and engaging user experience.
    """)
    
    st.subheader("Scalable Data Engineering")
    st.write("""
    Built with modern data engineering principles using Apache Airflow, MongoDB, PostgreSQL, 
    and follows medallion architecture patterns. Designed to handle growing data volumes 
    and complex analytical queries.
    """)

st.markdown("---")

# Architecture section
st.header("Technical Architecture")

st.write("**Data Pipeline Flow:**")
st.write("Spotify API → MongoDB (Raw Data) → Airflow ETL → PostgreSQL (Analytics) → Streamlit Dashboard")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Data Collection")
    st.write("- Spotify Web API")
    st.write("- Apache Airflow")
    st.write("- Python 3.11+")
    st.write("- OAuth 2.0 Authentication")

with col2:
    st.subheader("Data Storage")
    st.write("- MongoDB Atlas (Raw Data)")
    st.write("- PostgreSQL/Supabase (Analytics)")
    st.write("- Star Schema Design")
    st.write("- Medallion Architecture")

with col3:
    st.subheader("Visualization & Monitoring")
    st.write("- Streamlit Framework")
    st.write("- Plotly Interactive Charts")
    st.write("- Grafana Monitoring")
    st.write("- Automated Alerting")

st.markdown("---")

# What you can discover section
st.header("What You Can Discover")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Listening Patterns")
    st.write("When and how much you listen to music")
    
    st.subheader("Artist Preferences") 
    st.write("Your favorite artists and how your taste evolves")
    
    st.subheader("Mood Analysis")
    st.write("Emotional patterns through music choice")

with col2:
    st.subheader("Music Discovery")
    st.write("How you discover and adopt new music")
    
    st.subheader("Temporal Trends")
    st.write("Seasonal and time-based listening habits")
    
    st.subheader("Audio Features")
    st.write("Energy, danceability, and acoustic preferences")

st.markdown("---")

# Platform statistics
st.header("Platform Statistics")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Days of Data Collection", "100+", delta="Growing daily")

with col2:
    st.metric("ETL Pipeline Runs", "100+", delta="Automated daily")

with col3:
    st.metric("Data Processing Accuracy", "99.8%", delta="High reliability")

with col4:
    st.metric("Visualization Charts", "15+", delta="Interactive")

st.markdown("---")

# Core Architecture Highlights
st.header("🏗️ Core Architecture Highlights")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Modern Data Engineering Architecture")
    st.write("**Medallion Architecture**: Bronze (MongoDB) → Silver (Clean Staging) → Gold (DWH)")
    st.write("**Microservice Design**: Modular ETL components")
    st.write("**Cloud Native**: MongoDB Atlas + Supabase PostgreSQL")
    st.write("**Native Deployment**: Lightweight virtual environment-based deployment")

with col2:
    st.subheader("Data Pipeline Engineering")
    st.write("**Real-time ETL Pipeline**: Apache Airflow orchestrated daily automation")
    st.write("**Incremental Sync**: Intelligent timestamp-based incremental updates")
    st.write("**Error Handling & Retry**: Complete failure recovery strategy")
    st.write("**Batch Processing**: psycopg2 execute_values performance optimization")

st.markdown("---")

# Technical Innovation
st.header("🔧 Technical Innovation")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Multi-source Data Extraction")
    st.write("**Dual API Clients**: Python requests + cURL backup mechanism")
    st.write("**Smart Deduplication**: MongoDB upsert prevents duplicate data")
    st.write("**Batch Optimization**: Up to 50 tracks processed in single batch")

    st.subheader("Data Warehouse Design")
    st.write("**Star Schema**: Fact table + 4 dimension tables standard DWH design")
    st.write("**Time Series Optimization**: Dedicated date dimension table")
    st.write("**Pre-computed Aggregations**: Daily/weekly statistics pre-calculation")
    st.write("**Index Strategy**: Precision indexing for query patterns")

with col2:
    st.subheader("Advanced Data Quality Management")
    st.write("**Custom Validation Framework**: 5 major validation rule categories")
    st.write("**Real-time Quality Monitoring**: Quality checks integrated in ETL flow")
    st.write("**Data Lineage Tracking**: Complete end-to-end data flow recording")
    st.write("**Anomaly Detection**: Automatic quality issue flagging")

st.markdown("---")

# Data Governance
st.header("🔍 Data Governance")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Complete Data Lineage System")
    st.write("**Automatic Lineage Tracking**: Records every data transformation step")
    st.write("**Impact Analysis**: Upstream/downstream dependency analysis")
    st.write("**Network Graph Visualization**: NetworkX-based lineage relationship graph")
    st.write("**Execution Monitoring**: Real-time ETL execution status tracking")

with col2:
    st.subheader("Monitoring & Alerting System")
    st.write("**Grafana Monitoring**: System performance and data quality monitoring")
    st.write("**Automated Alerting**: Real-time notifications for anomalous conditions")
    st.write("**Metrics Tracking**: Key indicators like ETL success rate, data volume, execution time")
    st.write("**Visual Monitoring Dashboard**: Real-time system health status display")

st.markdown("---")

# GitHub and contact section
st.header("Source Code and Documentation")

col1, col2 = st.columns(2)

with col1:
    st.subheader("GitHub Repository")
    st.write("**Access the complete source code:**")
    st.write("This project is open source and available on GitHub.")

    st.link_button(
        "View Source Code", 
        "https://github.com/donydony228/SpotifyDataPipeline"
    )

with col2:
    st.subheader("Technical Documentation")
    st.write("**Project includes:**")
    st.write("- Complete setup instructions")
    st.write("- API documentation")
    st.write("- Database schema definitions")
    st.write("- Deployment guides")

st.markdown("---")

# Project context
col1, col2 = st.columns([3, 2])

with col1:
    st.header("About This Project")

    st.write("""
    This platform is developed by Desmond Peng. Serving as both a personal analytics tool and a portfolio demonstration of modern 
    data engineering practices, it showcases end-to-end data pipeline development, from API integration 
    and data collection through to advanced analytics and visualization.
    """)

with col2:
    st.image("dashboard/2fqdcir.jpg", width=500)

st.markdown("---")

# Footer
st.caption("Powered by Spotify Web API • Apache Airflow • MongoDB • PostgreSQL • Streamlit • Grafana")