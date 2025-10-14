FROM python:3.9-slim

WORKDIR /app

COPY dashboard /app

RUN pip install flask happybase

CMD ["python", "app.py"]
