import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import networkx as nx
import plotly.graph_objects as go

from database_manager import SupabaseManager
from sql_query.quality import bar_quality_load_data, bar_number_load_data


# Initialize database manager
db = SupabaseManager()

st.set_page_config(layout="wide")
st.title("Data Quality Dashboard")

st.header("Overview Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    # Today's Success Rate
    result = db.execute_query("SELECT avg(success_rate_percent) FROM dwh.v_data_lineage_full as d WHERE d.created_at > current_date group by source_column")
    st.metric("Today's Success Rate", f"{result['avg'][0]:.2f}%", border=True)

with col2:
    # Numero of Records Processed Today
    result = db.execute_query("SELECT sum(records_successful) FROM dwh.v_data_lineage_full as d WHERE d.created_at > current_date GROUP BY source_column")
    st.metric("Numero of Records Processed Today", result['sum'][0], border=True)

with col3:
    # Average Latency (seconds)
    result = db.execute_query("SELECT avg(execution_duration_seconds) FROM dwh.v_data_lineage_full as d WHERE d.created_at > current_date group by source_column")
    st.metric("Average Latency (seconds)", f"{result['avg'][0]:.2f}", border=True)

with col4:
    # Last Updated At
    result = db.execute_query("SELECT created_at FROM dwh.v_data_lineage_full as d WHERE d.created_at > current_date Limit 1")
    st.metric("Last Updated At", f"{result['created_at'][0]}", border=True)

st.markdown("---")

# Load data for treemap
days_to_display = st.slider(
    'Time Range (Days Back):', 
    min_value=1, 
    max_value=60, 
    value=14, 
    step=1
)

st.header("Data Lineage Over Time")

# Load data for bar chart
df_bar = bar_quality_load_data(days_to_display, db)

# Bar chart for average success rate over time
fig = px.bar(
    df_bar,
    x='played_date',
    y='avg_success_rate',
    labels={'played_date': 'Date', 'avg_success_rate': 'Average Success Rate (%)'},
    title=f'Average Success Rate Over the Past {days_to_display} Days'
)
fig.update_layout(xaxis_title='Date', yaxis_title='Average Success Rate (%)')
st.plotly_chart(fig, width='stretch')

# Load data for number of successful records
df_number = bar_number_load_data(days_to_display, db)

# Bar chart for number of successful records over time
fig2 = px.bar(
    df_number,
    x='played_date',
    y='successful_records',
    labels={'played_date': 'Date', 'successful_records': 'Number of Successful Records'},
    title=f'Number of Successful Records Over the Past {days_to_display} Days'
)
fig2.update_layout(xaxis_title='Date', yaxis_title='Number of Successful Records')
st.plotly_chart(fig2, width='stretch')

st.markdown("---")

# st.header("Data Lineage Details")

# # Load full data lineage details
# df_details = db.execute_query("SELECT source_node, target_node, edge_type, relationship_count, last_execution, avg_records_processed FROM dwh.v_lineage_graph ORDER BY last_execution DESC")

# G = nx.DiGraph()
# for _, row in df_details.iterrows():
#         G.add_edge(
#             row['source_node'], 
#             row['target_node'],
#             edge_type=row['edge_type'],
#             weight=row['relationship_count']
#         )

# # Use shell layout for better visualization
# pos = nx.shell_layout(G)
# edge_x = []
# edge_y = []
# for edge in G.edges():
#     x0, y0 = pos[edge[0]]
#     x1, y1 = pos[edge[1]]
#     edge_x.append(x0)
#     edge_x.append(x1)
#     edge_x.append(None)
#     edge_y.append(y0)
#     edge_y.append(y1)
#     edge_y.append(None)
# edge_trace = go.Scatter(
#     x=edge_x, y=edge_y,
#     line=dict(width=0.5, color='#888'),
#     hoverinfo='none',
#     mode='lines'
# )
# node_x = []
# node_y = []
# for node in G.nodes():
#     x, y = pos[node]
#     node_x.append(x)
#     node_y.append(y)
# node_trace = go.Scatter(
#     x=node_x, y=node_y,
#     mode='markers+text',
#     hoverinfo='text',
#     marker=dict(
#         showscale=True,
#         colorscale='YlGnBu',
#         size=10,
#         color=[],
#         colorbar=dict(
#             thickness=15,
#             title='Node Connections',
#             xanchor='left',
#             titleside='right'
#         ),
#         line_width=2
#     ),
#     text=[node for node in G.nodes()],
#     textposition="top center"
# )
# # Color nodes by number of connections
# node_adjacencies = []
# node_text = []
# for node in G.nodes():
#     adjacents = len(list(G.adj[node]))
#     node_adjacencies.append(adjacents)
#     node_text.append(f'{node} has {adjacents} connections') 
# node_trace.marker.color = node_adjacencies
# node_trace.text = [node for node in G.nodes()]
# fig = go.Figure(data=[edge_trace, node_trace],
#              layout=go.Layout(
#                 title='<br>Data Lineage Graph',
#                 titlefont_size=16,
#                 showlegend=False,
#                 hovermode='closest',
#                 margin=dict(b=20,l=5,r=5,t=40),
#                 annotations=[ dict(
#                     text="Data Lineage Visualization",
#                     showarrow=False,
#                     xref="paper", yref="paper",
#                     x=0.005, y=-0.002 ) ],
#                 xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
#                 yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
#                 )
# st.plotly_chart(fig, width='stretch')