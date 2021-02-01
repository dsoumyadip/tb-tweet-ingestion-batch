import logging
import os
import time
from datetime import datetime

import requests
from google.cloud import firestore

from update_entity_sentiment import get_entity_sentiment

logging.basicConfig(level=logging.INFO)

# Fetch 100 tweets at a single API call
TWEET_BATCH_SIZE = 100


def auth():
    return os.environ.get("BEARER_TOKEN")


def get_handles():
    """Get list of user ids of Twitter handles
    Returns:
        List of user ids for Twitter handles
    """
    db = firestore.Client()
    h_ref = db.collection(u'tb-handles')
    logging.info("Getting list of handles...")
    docs = h_ref.stream()
    list_of_handles = dict()
    for doc in docs:
        list_of_handles[doc.to_dict()['username']] = doc.to_dict()['id']
    return list_of_handles


def create_url(user_id):
    """
    Returns endpoint to get historical tweets of a handle
    Args:
        user_id: User Id of Twitter handle

    Returns:
        Endpoint where to make request

    """
    return f"https://api.twitter.com/2/users/{user_id}/tweets"


def get_params(next_token, batch_size=100):
    """
    List of required fields of tweet objects
    Args:
        next_token: Token for pagination
        batch_size: Number of tweets to retrieve at each API call

    Returns:
        Dictionary containing request parameters

    """
    if next_token is None:
        return {"tweet.fields": "id,text,author_id,conversation_id,"
                                "created_at,geo,in_reply_to_user_id,lang,"
                                "public_metrics,source",
                "max_results": batch_size}
    else:
        return {"tweet.fields": "id,text,author_id,conversation_id,"
                                "created_at,geo,in_reply_to_user_id,lang,"
                                "public_metrics,source",
                "max_results": batch_size,
                "pagination_token": next_token}


def create_headers(bearer_token):
    """Create headers to make API call
    Args:
        bearer_token: Bearer token

    Returns:
        Header for API call
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def connect_to_endpoint(url, headers, params):
    """This function fetches tweets from specific twitter handle timelines
    Args:
        url: API endpoint to fetch tweets
        headers: Headers
        params: Additional parameters

    Returns:
        List of tweets as json payload
    """
    response = requests.request("GET", url, headers=headers, params=params)
    logging.info("Connected to ingestion endpoint")
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def ingest_tweets_to_firestore(tweets):
    """Ingest tweets to firestore db
    Args:
        tweets: List of tweets in dict format
    """
    db = firestore.Client()
    batch = db.batch()

    for tweet in tweets:
        twt_ref = db.collection(u'tb-tweets').document(tweet['id'])
        batch.set(twt_ref, tweet)
    batch.commit()


def update_username_as_key(tweet_dict, username):
    """
    Set each tweet object's username as key of that document
    Args:
        tweet_dict: Dictionary containing tweets
        username: username what need to be set

    Returns:
        Tweet containing dictionary

    """
    tweet_dict['username'] = username
    tweet_dict['last_updated'] = datetime.now()
    tweet_dict['update_type'] = 'batch'
    return tweet_dict


def ingest_tweets_batch():
    bearer_token = auth()
    handles_dict = get_handles()
    headers = create_headers(bearer_token)

    # Creating reverse dict to get id->username mapping
    rev_handles_dict = {v: k for k, v in handles_dict.items()}

    # List of handles to fetch historical tweets
    # handles = list(rev_handles_dict.keys())  # For PROD
    handles = ['18839785', '3171712086', '1652541', '51241574']  # For testing only

    logging.info("Started Ingesting tweets...")
    for ticker in handles:
        logging.info(f"Ingesting tweets for @{rev_handles_dict[ticker]}")
        url = create_url(ticker)
        next_token = None
        tw_cnt = 0
        while True:
            params = get_params(next_token=next_token, batch_size=TWEET_BATCH_SIZE)
            try:
                json_response = connect_to_endpoint(url, headers, params)
                next_token = json_response["meta"].get("next_token")
                if next_token is None:
                    break
                json_response_with_username = map(lambda x: update_username_as_key(x, rev_handles_dict[ticker]),
                                                  json_response["data"])
                updated_tweet_with_entity_sentiment = map(lambda x: get_entity_sentiment(x),
                                                          json_response_with_username)
                ingest_tweets_to_firestore(updated_tweet_with_entity_sentiment)
                tw_cnt += len(json_response["data"])
                logging.info(f"Ingested {tw_cnt} tweets for @{rev_handles_dict[ticker]}")
                time.sleep(2)
            except Exception as e:
                logging.info(e)
                time.sleep(10)


if __name__ == "__main__":
    ingest_tweets_batch()
