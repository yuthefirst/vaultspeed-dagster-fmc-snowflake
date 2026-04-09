FROM python:3.12-slim

WORKDIR /app

ENV PROJECT_NAME=DataWranglers

RUN mkdir /dagster/

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kris.py /app/

CMD ["python", "kris.py"]