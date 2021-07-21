import twint
import pandas as pd


def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_latest_tweets_from_handle(username, num_tweets):
    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    #c.Since = date
    c.Hide_output = True
    twint.run.Search(c)
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags',
                                    'username', 'name', 'link', 'urls', 'photos', 'video',
                                    'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except Exception as e:
        print(e)
        tweet_df = pd.DataFrame()
    return tweet_df



a = get_latest_tweets_from_handle("wilsonukeme", 200)
print(a)