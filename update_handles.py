# This script takes list of Twitter handles to tracks and updates corresponding info in Firestore DB

import datetime
import logging
import os

import requests
from google.cloud import firestore
from google.cloud import storage

from utility import BUCKET_NAME, SOURCE_BLOB_NAME

logging.basicConfig(level=logging.INFO)


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_headers(bearer_token):
    headers = {"Authorization": f"Bearer {bearer_token}"}
    return headers


def get_list_of_handles():
    """
    Fetches list of handles to track
    Returns:
        List of Twitter Handles
    """
    logging.info("Creating cloud storage client...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    file_path = '/tmp/twitter_handles.txt'  # For Google cloud functions only
    # file_path = 'tmp/twitter_handles.txt'
    blob.download_to_filename(file_path)

    with open(file_path, "r") as read_file:
        users = read_file.read().splitlines()
        logging.info(f"Total number of Twitter handle to track: {len(users)}")
    return users


def create_url(users):
    """
    Create GET endpoint to get handle information
    Args:
        users: List of twitter handles

    Returns:
        URL for GET call to get handle information
    """
    str1 = ','.join(users)
    usernames = "usernames=" + str1
    user_fields = "user.fields=description,created_at,location,pinned_tweet_id,profile_image_url,protected," \
                  "public_metrics,url,verified"
    url = f"https://api.twitter.com/2/users/by?{usernames}&{user_fields}"
    return url


def connect_to_endpoint(url, headers):
    """
    Fetches latest information of a Twitter handle
    Args:
        url: URL to make a GET call
        headers: Headers need for authentication

    Returns:
        List of tweets in json format
    """
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def update_in_firestore(data):
    """
    Update latest handle info in Google Firestore
    Args:
        data: Dictionary containing list of tweets

    Returns:
        None
    """
    logging.info("Creating Firestore client...")
    db = firestore.Client()

    logging.info("Updating user profiles...")
    for user_info in data['data']:
        user_info['last_updated'] = datetime.datetime.now()
        db.collection('tb-handles').document(user_info['username']).set(user_info)
    logging.info("Update completed")


def main(request):
    bearer_token = auth()
    users = get_list_of_handles()
    url = create_url(users=users)
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)
    update_in_firestore(json_response)
    return "Success"
