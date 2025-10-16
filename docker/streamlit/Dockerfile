# docker/streamlit/Dockerfile

FROM python:3.10-slim

WORKDIR /app

# Cài các thư viện cần thiết
RUN pip install streamlit pandas plotly pymongo

# Copy toàn bộ code (hoặc chỉ batch nếu muốn gọn hơn)
COPY . /app

# Cổng cho Streamlit
EXPOSE 8501

# Lệnh chạy Streamlit
CMD ["streamlit", "run", "batch/streamlit_batch_dashboard.py", "--server.address=0.0.0.0"]
