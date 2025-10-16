import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide", page_title="Crypto Batch Predictions")

st.title("Crypto Batch Insights")
st.subheader("Long-term Predictions from Batch Layer")

# Kết nối MongoDB (dùng container hostname nếu chạy qua Docker)
mongo_uri = "mongodb://mongodb:27017"  # nếu chạy trong container
client = MongoClient(mongo_uri)
collection = client["crypto_batch"]["predictions"]

# Load dữ liệu
data = list(collection.find())
df = pd.DataFrame(data)

if df.empty:
    st.warning("No data found in MongoDB.")
    st.stop()

df["symbol"] = df["symbol"].str.upper()

# ======= BẢNG DỮ LIỆU =======
st.markdown("### Full Prediction Table")
st.dataframe(df.set_index("symbol"))

# ======= 2 BIỂU ĐỒ CỘT: PRICE + RMSE =======
cols = st.columns(2)

with cols[0]:
    st.markdown("#### Predicted Price by Symbol")
    fig1 = px.bar(df, x="symbol", y="predicted_price", text="predicted_price", color="symbol")
    st.plotly_chart(fig1, use_container_width=True)

with cols[1]:
    st.markdown("#### Model RMSE by Symbol")
    fig2 = px.bar(df, x="symbol", y="rmse", text="rmse", color="symbol")
    st.plotly_chart(fig2, use_container_width=True)

# ======= SCATTER =======
st.markdown("### Scatter Plot: Price vs RMSE")
fig3 = px.scatter(df, x="predicted_price", y="rmse", color="symbol", size="rmse", hover_name="symbol")
st.plotly_chart(fig3, use_container_width=True)

# ======= BOX PLOT =======
st.markdown("### Price Distribution (Box Plot)")
fig4 = px.box(df, y="predicted_price", points="all", color="symbol")
st.plotly_chart(fig4, use_container_width=True)

# ======= HISTOGRAM =======
st.markdown("### Histogram of RMSE")
fig5 = px.histogram(df, x="rmse", nbins=10, color="symbol")
st.plotly_chart(fig5, use_container_width=True)
