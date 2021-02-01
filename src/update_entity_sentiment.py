import re

from helper import TYPE_, ENCODING_TYPE, CLIENT, retry


def clean_tweet(tweet):
    """These functions cleans each tweet string by removing any character other that alphanumeric chars.
    Args:
        tweet: Tweet message in string

    Returns:
        Cleaned message in string
    """

    cleaned_tweet = re.sub('[^A-Za-z0-9\s+]+', '', tweet)
    return cleaned_tweet


@retry
def get_entity_sentiment(tweet):
    """Get entity and sentiment of a tweet
    Args:
        tweet: The tweet object

    Returns:
        Tweet object updated with entity and sentiment.
    """
    # language = "en"  # Will be detected automatically by cloud NLP API
    document = {"content": clean_tweet(tweet["text"]), "type_": TYPE_}
    request_body = {'document': document, 'encoding_type': ENCODING_TYPE}

    sentiment_response = CLIENT.analyze_sentiment(request=request_body)
    entity_response = CLIENT.analyze_entities(request=request_body)

    tweet["sentiment"] = dict()
    tweet["entities"] = []
    tweet["sentiment"]["score"] = sentiment_response.document_sentiment.score
    tweet["sentiment"]["magnitude"] = sentiment_response.document_sentiment.magnitude

    # Get sentiment for all sentences in the document
    for entity in entity_response.entities:
        tweet["entities"].append(entity.name)
    return tweet
