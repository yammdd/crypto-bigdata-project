FROM python:3.9-slim

WORKDIR /app

COPY dashboard /app
COPY ../../dashboard/static /app/static
COPY ../../models /app/models

RUN pip install flask happybase

CMD ["python", "app.py"]
