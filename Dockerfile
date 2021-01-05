FROM python:3.7

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ingest_tweets_batch.py .
COPY utility.py .

CMD ["python", "ingest_tweets_batch.py"]
