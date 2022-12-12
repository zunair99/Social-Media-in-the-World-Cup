# Import the necessary libraries
import tweepy
import os
import pandas as pd
import datetime

def auth(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret):

    # Initialize the Tweepy API client
    # Specifications:
    # - Return type is a dictionary
    # - Wait on rate limit to avoid getting rate limited
    # - Notify when rate limit is hit
    client = tweepy.Client(
        bearer_token=bearer_token, 
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        return_type="dict",
        wait_on_rate_limit=True
        )
    print("Twitter API Client Initialized.")
    return client

def get_tweets(client):

    # Get the 100 most recent tweets from the FIFA World Cup account
    # Specifications:
    # - Exclude retweets
    # - Include tweet text, id, and non-public metrics (Total Number of Impressions Generated for the Tweet)
    dict = client.search_recent_tweets(
        query="from:FIFAWorldCup -is:retweet", 
        tweet_fields="id,text,attachments,public_metrics,source,created_at",
        max_results=100,
        user_auth=True)
    print("Tweets Retrieved.")
    
    # Convert the dictionary to a Pandas DataFrame
    df = pd.DataFrame.from_dict(dict[0])
    print("Tweets Converted to DataFrame.")

    # Add the extraction date to the DataFrame
    df['extraction_date'] = datetime.datetime.now()
    print("Extraction Date Added to DataFrame.")

    return df

def transform_df(df):

    # Convert the attachments column from a dictionary to columns
    df = pd.concat([df.drop(['attachments'], axis=1), df['attachments'].apply(pd.Series)], axis=1)

    # Drop unnecessary columns
    df.drop(0, axis = 1)

    # Convert the media_keys column from a string to a Boolean object
    df['media_check'] = df['media_keys'].isna().replace({True: 'No Media', False: 'Media'})

    # Convert the poll_ids column from a string to a Boolean object
    df['poll_check'] = df['poll_ids'].isna().replace({True: 'No Poll', False: 'Poll'})

    # Convert the public_metrics column from a dictionary to columns and append to the DataFrame
    df = pd.concat([df.drop(['public_metrics'], axis=1), df['public_metrics'].apply(pd.Series)], axis=1)
    print("Public Metrics Series converted to Columns and Added to DataFrame.")

    # Convert the edit_history_tweet_ids column from a string to a Boolean object
    def get_edit_history():
        for i in range(len(df['edit_history_tweet_ids'])):
            if len(df['edit_history_tweet_ids'][i]) > 1:
                return df['edit_history_tweet_ids'][i]
            else:
                return None
    
    # Get the edit history for each tweet, add to a new column in the DataFrame
    df['edit_history']=get_edit_history()
    print("Edit History Boolean Added to DataFrame.")
    return df

def main():

    # Get the Twitter API credentials from the environment variables
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    consumer_key = os.environ.get("TWITTER_API_KEY")
    consumer_secret = os.environ.get("TWITTER_KEY_SECRET")
    access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
    access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
    
    # Initialize the Twitter API client
    client = auth(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret)
    
    # Get the tweets from the FIFA World Cup account
    df = get_tweets(client)

    # Transform the DataFrame and return the result
    return transform_df(df)

if __name__ == '__main__':

    # Run the main function
    main()