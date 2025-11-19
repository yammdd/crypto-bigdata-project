FROM python:3.10-slim

WORKDIR /app

RUN pip install streamlit pandas plotly pymongo

COPY . /app

EXPOSE 8501

CMD ["streamlit", "run", "batch/streamlit_batch_dashboard.py", "--server.address=0.0.0.0"]