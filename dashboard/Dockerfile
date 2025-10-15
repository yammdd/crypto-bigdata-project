FROM python:3.9-slim

WORKDIR /app

COPY dashboard/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard /app
COPY ../../dashboard/static /app/static
COPY ../../models /app/models

CMD ["python", "app.py"]