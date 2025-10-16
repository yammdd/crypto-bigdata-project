# batch/streamlit_batch_dashboard.py

import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide", page_title="Crypto Batch Predictions")

st.title("💹 Crypto Batch Insights")
st.subheader("🔮 Long-term Predictions from Batch Layer (XGBoost)")

# Kết nối MongoDB
mongo_uri = "mongodb://mongodb:27017"
client = MongoClient(mongo_uri)
collection = client["crypto_batch"]["predictions"]

# Tải dữ liệu
data = list(collection.find())
df = pd.DataFrame(data)

if df.empty:
    st.error("❌ Không tìm thấy dữ liệu trong MongoDB.")
    st.stop()

# Debug nếu cần
# st.write("🔍 Các cột có trong MongoDB:", df.columns.tolist())

df["symbol"] = df["symbol"].str.upper()

# Bảng dữ liệu đầy đủ
st.markdown("### 📊 Full Prediction Table")
st.dataframe(df.set_index("symbol"), use_container_width=True)

# Giới hạn scale để dễ đọc
df_limited = df.copy()
df_limited["predicted_price_capped"] = df_limited["predicted_price"].apply(lambda x: min(x, 5000))
df_limited["rmse_capped"] = df_limited["rmse"].apply(lambda x: min(x, 100))

# Biểu đồ cột
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📈 Predicted Price (Capped)")
    fig1 = px.bar(df_limited, x="symbol", y="predicted_price_capped", color="symbol", text="predicted_price")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown("### 🎯 Model RMSE (Capped)")
    fig2 = px.bar(df_limited, x="symbol", y="rmse_capped", color="symbol", text="rmse")
    st.plotly_chart(fig2, use_container_width=True)

# Scatter plot
st.markdown("### 🔬 Scatter Plot: Predicted Price vs RMSE")
fig3 = px.scatter(df, x="predicted_price", y="rmse", color="symbol", hover_name="symbol")
st.plotly_chart(fig3, use_container_width=True)

# Box plot
st.markdown("### 📦 Price Distribution (Box Plot)")
fig4 = px.box(df, y="predicted_price", points="all", color="symbol")
st.plotly_chart(fig4, use_container_width=True)

# Pie chart (chỉ nếu có volume)
if "volume" in df.columns:
    st.markdown("### 🍰 Volume Distribution (Pie Chart)")
    fig5 = px.pie(df, names="symbol", values="volume", title="Crypto Volume Share")
    st.plotly_chart(fig5, use_container_width=True)

# Dự đoán vs thực tế (nếu có)
if "last_price" in df.columns:
    st.markdown("### 📉 Predicted vs Last Price (Line Chart)")
    fig6 = px.line(
        df.melt(id_vars=["symbol"], value_vars=["last_price", "predicted_price"]),
        x="symbol", y="value", color="variable", markers=True
    )
    st.plotly_chart(fig6, use_container_width=True)
