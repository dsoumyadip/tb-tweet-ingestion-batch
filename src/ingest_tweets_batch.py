import logging
import os

import requests
from google.cloud import firestore

logging.basicConfig(level=logging.INFO)


def auth():
    return os.environ.get("BEARER_TOKEN")


def get_handles():
    """Get list of user ids of Twitter handles
    Returns:
        List of user ids for Twitter handles
    """
    db = firestore.Client()
    h_ref = db.collection((u'tb-handles',))
    logging.info("Getting list of handles...")
    docs = h_ref.stream()
    list_of_handles = dict()
    for doc in docs:
        list_of_handles[doc.to_dict()['username']] = doc.to_dict()['id']
    return list_of_handles


def create_url(user_id):
    """Returns endpoint to get historical tweets of a handle
    """
    return f"https://api.twitter.com/2/users/{user_id}/tweets"


def get_params():
    """List of required fields of tweet objects
    """
    return {"tweet.fields": "id,text,author_id,conversation_id,"
                            "created_at,geo,in_reply_to_user_id,lang,"
                            "public_metrics,source"}


def create_headers(bearer_token, next_token=None):
    """Create headers to make API call
    Args:
        bearer_token: Bearer token
        next_token: Token for pagination

    Returns:
        Header for API call
    """
    if next_token is None:
        headers = {"Authorization": f"Bearer {bearer_token}"}
    else:
        headers = {"Authorization": f"Bearer {bearer_token}", "next_token": next_token}
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
        twt_ref = db.collection((u'tb-tweets',)).document(tweet['id'])
        batch.set(twt_ref, tweet)
    batch.commit()


def update_username_as_key(tweet_dict, username):
    tweet_dict['username'] = username
    return tweet_dict


def ingest_tweets_batch():
    bearer_token = auth()
    params = get_params()
    handles_dict = get_handles()

    # Creating reverse dict to get id->username mapping
    rev_handles_dict = {v: k for k, v in handles_dict.items()}

    # List of handles to fetch historical tweets
    handles = list(rev_handles_dict.keys())  # For PROD
    # handles = ['223106342', '2244994945']  # For testing only

    logging.info("Started Ingesting tweets...")
    for ticker in handles:
        logging.info(f"Ingesting tweets for @{rev_handles_dict[ticker]}")
        tw_cnt = 10  # Fetch 10 tweets at a single API call
        next_token = None
        url = create_url(ticker)
        while True:
            headers = create_headers(bearer_token, next_token)
            json_response = connect_to_endpoint(url, headers, params)
            next_token = json_response["meta"].get("next_token")
            if next_token is None:
                break
            json_response_with_username = map(lambda x: update_username_as_key(x, rev_handles_dict[ticker]),
                                              json_response["data"])
            ingest_tweets_to_firestore(json_response_with_username)
            logging.info(f"Ingested {tw_cnt} tweets for @{rev_handles_dict[ticker]}")
            tw_cnt += 10


if __name__ == "__main__":
    ingest_tweets_batch()
