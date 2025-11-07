# streamlit run dashboard/app.py
import streamlit as st
import numpy as np
import pandas as pd
from database_manager import SupabaseManager

db = SupabaseManager()
query = """
            select track_name, COUNT(track_name) as Freq
            from dwh.fact_listening as f
            left join dwh.dim_tracks as t
            on f.track_key = t.track_key
            -- where f.played_at > "20251025"
            group by track_name
            order by COUNT(track_name) desc
            """
df = db.execute_query(query)
st.title("數據分析展示範例")

st.dataframe(df)
# 隨機生成數據
data = np.random.randn(100, 2)
df = pd.DataFrame(data, columns=["變數A", "變數B"])
st.write("隨機數據：")
# st.dataframe(df)
st.write("線圖展示：")
st.line_chart(df)
st.write("直方圖展示：")
st.bar_chart(df)