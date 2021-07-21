import twint
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
import os
from pymongo import MongoClient
from datetime import datetime, timedelta
from aiohttp.client_exceptions import ClientOSError
#from config.settings import MONGO_URL
from time import sleep
pd.options.mode.chained_assignment = None

#client = MongoClient(MONGO_URL)
#db = client.rss_extraction

# import nest_asyncio
# nest_asyncio.apply()



def get_date():
    """
     Build functionality to get the date 24 hours from now
    """
    today = datetime.now()
    yday = today + timedelta(-1)
    last_24_date = yday.date()
    last_24_hour = yday.hour
    minute = '00'
    second = '00'
    last_24_date_str = '%s %s:%s:%s' % (last_24_date, last_24_hour, minute, second)
    return last_24_date_str


def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_latest_tweets_from_handle(username, num_tweets, date):
    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
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


def get_latest_tweets_for_publications(rss_publication_df, num_tweets, since_date_str):
    ## Get the latest tweets that have been published by these publications in the last 3 hours
    latest_tweet_df_list = []
    for i in range(len(rss_publication_df.head(30))):
    
        try:
            rss_twitter_handle = rss_publication_df['Publication Handle'].iloc[i]
            print(rss_twitter_handle)
            tweet_df = get_latest_tweets_from_handle(rss_twitter_handle, num_tweets, since_date_str)
            tweet_df['twitter_handle'] = rss_twitter_handle
            print()
            if len(tweet_df) > 0:
                tweet_df = tweet_df.sort_values(by='nreplies', ascending=False)
                latest_tweet_df_list.append(tweet_df)
                
        except Exception as e:
            print(e)
            ...
    ## Now run through all the tweets and filter out those that dont have article links
    try:
        latest_tweets_df = pd.concat(latest_tweet_df_list, sort=True)
    except:
        latest_tweets_df = pd.DataFrame()
    tweet_w_link_indices = []
    link_tweet_w_comment_indices = []
    for ii in range(len(latest_tweets_df)):
        tweet_text = latest_tweets_df.iloc[ii]['tweet']
        tweet_urls = latest_tweets_df.iloc[ii]['urls']
        num_tweet_comments = latest_tweets_df.iloc[ii]['nreplies']
        if len(tweet_urls) > 0:
            tweet_w_link_indices.append(ii)
    article_tweets_df = latest_tweets_df.iloc[tweet_w_link_indices]
    
    return article_tweets_df


def last_n_hours(num):
    time_diff = num
    today = datetime.now() 
    date = today.date()
    since_hour = max(0, today.hour - time_diff)
    minute = today.minute
    second = '00'
    since_date_str = '%s %s:%s:%s' % (date, since_hour, minute, second)

    return since_date_str


# def save_to_filtered_collection(df):
#     rss_collection = db.rss_collection
#     cur = rss_collection.count_documents({})
#     print('We had %s  entries at the start' % cur)
    
    
#     id_list=list(rss_collection.find({}, {"_id": 0, "id": 1}))
#     id_list=list((val for dic in id_list for val in dic.values()))
    
#     print(len(df))
#     for dfs in df.to_dict('records'):
#         if dfs['id'] not in id_list:
#             print(dfs['urls'][0])
#             url = dfs['urls'][0]

#             result = get_summary(url)
#             sleep(5)

#             data = {**dfs, **result}
#             rss_collection.insert_one(data)

#     cur = rss_collection.count_documents({})
#     print('We have %s  entries at the end' % cur)


#     return None


def get_artilces_30_to_45_min_old(rss_publication_df, num_tweets, last_hour_date_str):
    all_articles = get_latest_tweets_for_publications(rss_publication_df, num_tweets, last_hour_date_str)
    
    all_articles["run_time"] = datetime.now()
    all_articles["run_time"] = all_articles.apply(lambda row: (datetime.strftime(row["run_time"], '%Y-%m-%d %H:%M:%S')),axis=1)
    
    all_articles['date'] = pd.to_datetime(all_articles['date'], format = '%Y-%m-%d %H:%M:%S')
    
    all_articles['time_since_tweet'] = all_articles.apply(lambda row: (datetime.strptime(row.run_time, '%Y-%m-%d %H:%M:%S') - row.date).total_seconds()/60, axis=1)
    
    all_articles = all_articles[(all_articles['time_since_tweet'] <= 45) & (all_articles['time_since_tweet'] >= 30)]

    
    return all_articles


def run_processes():
    rss_twitter_handles_path = os.path.join(os.getcwd(), 'launch_rss_publications_new_edit.csv')
    rss_publication_df = pd.read_csv(rss_twitter_handles_path)
    #rss_publication_df = ["DisruptAfrica", "RWW", "WSJVC", "Recode", "CNET", "afrotech", "politico", "bbchealth", "bbcnews", "bbcworld", "bbctech", "BBCSport", "nytimes"]
    num_tweets = 1000
    last_hour_date_str = last_n_hours(1)

    processed_tweet = get_artilces_30_to_45_min_old(rss_publication_df, num_tweets, last_hour_date_str)

    return processed_tweet


#save_to_filtered_collection(processed_tweet)



############################################################################################
############################################################################################