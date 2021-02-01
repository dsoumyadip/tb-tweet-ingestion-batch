FROM python:3.7

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src src/

CMD ["python", "src/ingest_tweets_batch.py"]
