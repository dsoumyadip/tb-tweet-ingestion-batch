import logging
import os

import requests
from google.cloud import firestore

logging.basicConfig(level=logging.INFO)


def auth():
    return os.environ.get("BEARER_TOKEN")


def get_handles():
    """
    Get list of ids of Twitter handles
    Returns:
        List of ids for Twitter handles
    """
    db = firestore.Client()
    h_ref = db.collection(u'tb-handles')
    logging.info("Getting lis of handles...")
    docs = h_ref.stream()
    list_of_handles = [doc.to_dict()['id'] for doc in docs]
    return list_of_handles


def create_url(user_id):
    return f"https://api.twitter.com/2/users/{user_id}/tweets"


def get_params():
    """
    Returns:
        List of fields required of tweet objects

    """
    return {"tweet.fields": "id,text,author_id,conversation_id,"
                            "created_at,geo,in_reply_to_user_id,lang,"
                            "public_metrics,source"}


def create_headers(bearer_token, next_token=None):
    """
    Create headers
    Args:
        bearer_token: Bearer token
        next_token: Token for pagnation

    Returns:
        Header for api call
    """
    if next_token is None:
        headers = {"Authorization": f"Bearer {bearer_token}"}
    else:
        headers = {"Authorization": f"Bearer {bearer_token}", "next_token": next_token}
    return headers


def connect_to_endpoint(url, headers, params):
    """
    This function fetches tweets from specific twitter handle timelines
    Args:
        url: API endpoint to fetch tweets
        headers: Headers
        params: Additional parameters

    Returns:
        List of tweets as json payload
    """
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def ingest_tweets_to_firestore(tweets):
    """
    Ingest tweets to firestore db
    Args:
        tweets: List of tweets in dict format

    Returns:
        None
    """
    db = firestore.Client()
    batch = db.batch()

    for tweet in tweets:
        twt_ref = db.collection(u'tb-tweets').document(tweet['id'])
        batch.set(twt_ref, tweet)

    batch.commit()


def ingest_tweets_batch(request):
    bearer_token = auth()
    params = get_params()
    handles = get_handles()
    logging.info("Started Ingesting tweets...")
    handles = ['223106342', '2244994945']
    for ticker in handles:
        logging.info(f"Ingesting tweets for {ticker}")
        tw_cnt = 10
        next_token = None
        url = create_url(ticker)
        while True:
            headers = create_headers(bearer_token, next_token)
            json_response = connect_to_endpoint(url, headers, params)
            next_token = json_response["meta"].get("next_token")
            if next_token is None:
                break
            ingest_tweets_to_firestore(json_response["data"])
            logging.info(f"Ingested {tw_cnt} tweets for {ticker}")
            tw_cnt += 10
            import time
            time.sleep(10)


# if __name__ == "__main__":
#     ingest_tweets_batch()
