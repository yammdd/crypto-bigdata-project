import streamlit as st
from pymongo import MongoClient
import pandas as pd
import numpy as np
import plotly.express as px

st.set_page_config(layout="wide", page_title="Crypto Batch Dashboard")

st.title("Crypto Batch Analytics Dashboard")
st.subheader("Long-term Predictions from Batch Layer (MongoDB)")

# Kết nối MongoDB
client = MongoClient("mongodb://mongodb:27017")
collection = client["crypto_batch"]["predictions"]

# Lấy dữ liệu
data = list(collection.find())
df = pd.DataFrame(data)

if df.empty:
    st.error("Không tìm thấy dữ liệu trong MongoDB.")
    st.stop()

df["symbol"] = df["symbol"].str.upper()

# Giới hạn ngoại lai
clip_cols = ["predicted_price", "last_price", "volume", "volatility"]
for c in clip_cols:
    if c in df.columns:
        df[c] = np.clip(df[c], 0, np.percentile(df[c], 95))

st.markdown("### Full Prediction Dataset")
st.dataframe(df.set_index("symbol"), use_container_width=True)

# ===== Biểu đồ cơ bản =====
col1, col2 = st.columns(2)
with col1:
    st.markdown("### Predicted vs Last Price")
    fig1 = px.bar(df, x="symbol", y=["predicted_price", "last_price"], barmode="group", text_auto=True)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown("### Model RMSE")
    fig2 = px.bar(df, x="symbol", y="rmse", color="symbol", text="rmse")
    st.plotly_chart(fig2, use_container_width=True)

# ===== Khám phá đặc trưng =====
st.markdown("### Feature Exploration")
tabs = st.tabs(["Scatter", "Boxplot", "Pie", "Correlation", "Histogram", "Price Change"])

with tabs[0]:
    fig = px.scatter(df, x="volatility", y="predicted_price", color="symbol",
                     size="volume", hover_data=[
                         "ma_6h", "ma_24h", "ma_72h", 
                         "price_change_pct", "volume_change_pct",
                         "high_low_ratio", "high_close_diff", "low_close_diff"
                     ])
    st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    box_features = ["open", "high", "low", "ma_6h", "ma_24h", "ma_72h", "predicted_price"]
    fig = px.box(df, y=box_features, points="all", color="symbol")
    st.plotly_chart(fig, use_container_width=True)

with tabs[2]:
    fig = px.pie(df, names="symbol", values="volume", title="Volume Share per Coin")
    st.plotly_chart(fig, use_container_width=True)

with tabs[3]:
    corr = df.select_dtypes("number").corr()
    fig = px.imshow(corr, text_auto=True, color_continuous_scale="Blues", title="Feature Correlation Heatmap")
    st.plotly_chart(fig, use_container_width=True)

with tabs[4]:
    fig = px.histogram(df, x="volatility", color="symbol", nbins=25)
    st.plotly_chart(fig, use_container_width=True)

with tabs[5]:
    fig = px.bar(df, x="symbol", y="price_change_pct", color="symbol", text="price_change_pct")
    st.plotly_chart(fig, use_container_width=True)
