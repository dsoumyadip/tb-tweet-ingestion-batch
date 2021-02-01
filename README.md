# tb-tweet-ingestion

This application fetches tweets from Twitter Timelines and stores in Firestore.

* This is a batch operation. Invoke once to get historical tweets.

## How to run:

Execute this command in a bash shell to run:  

```shell
docker run -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/SERVICE-ACCOUNT-KEY.json -e BEARER_TOKEN=TWITTER_API_BEARER_TOKEN -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/SERVICE-ACCOUNT-KEY.json:ro  gcr.io/sharp-haven-301406/tb-ingest-tweets-batch
```

*  Make sure **GOOGLE_APPLICATION_CREDENTIALS** is set in local, so that it can bind local service account key with docker volume.
