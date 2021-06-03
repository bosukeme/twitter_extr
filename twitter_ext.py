import pandas as pd
import twint
import os
from datetime import datetime
import nest_asyncio
from time import sleep
pd.options.mode.chained_assignment = None
nest_asyncio.apply()


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
    sleep(5)
    return tweet_df


def get_latest_tweets_for_publications(rss_publication_df, num_tweets, since_date_str):
    ## Get the latest tweets that have been published by these publications in the last 3 hours
    latest_tweet_df_list = []
    for i in range(len(rss_publication_df)):
        try:
            rss_twitter_handle = rss_publication_df['Publication Handle'].iloc[i]
            print(rss_twitter_handle)
            tweet_df = get_latest_tweets_from_handle(rss_twitter_handle, num_tweets, since_date_str)
            tweet_df['twitter_handle'] = rss_twitter_handle
            print(len(tweet_df))
            print()
            if len(tweet_df) > 0:
                tweet_df = tweet_df.sort_values(by='nreplies', ascending=False)
                latest_tweet_df_list.append(tweet_df)
        except Exception as e:
            print(e)
            ...
    ## Now run through all the tweets and filter out those that dont have article links
    latest_tweets_df = pd.concat(latest_tweet_df_list, sort=True)
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


def get_artilces_30_to_45_min_old(rss_publication_df, num_tweets, last_hour_date_str):
    all_articles = get_latest_tweets_for_publications(rss_publication_df, num_tweets, last_hour_date_str)
    
    all_articles["run_time"] = datetime.now()
    all_articles["run_time"] = all_articles.apply(lambda row: (datetime.strftime(row["run_time"], '%Y-%m-%d %H:%M:%S')),axis=1)
    
    all_articles['date'] = pd.to_datetime(all_articles['date'], format = '%Y-%m-%d %H:%M:%S')
    
    all_articles['time_since_tweet'] = all_articles.apply(lambda row: (datetime.strptime(row.run_time, '%Y-%m-%d %H:%M:%S') - row.date).total_seconds()/60, axis=1)
    
    all_articles = all_articles[(all_articles['time_since_tweet'] <= 45) & (all_articles['time_since_tweet'] >= 30)]

    
    return all_articles

rss_twitter_handles_path = os.path.join(os.getcwd(), 'launch_rss_publications.csv')
rss_publication_df = pd.read_csv(rss_twitter_handles_path)
num_tweets = 1000
last_hour_date_str = last_n_hours(1)

processed_tweet = get_artilces_30_to_45_min_old(rss_publication_df, num_tweets, last_hour_date_str)
processed_tweet['total'] = processed_tweet.apply(lambda row: row['nlikes'] + row['nreplies'] + row['nretweets'], axis=1)

dff = pd.read_csv("average_reaction_monday.csv")

new_df_2 = pd.merge(processed_tweet, dff, on='username')

new_df_2['cut_off'] = new_df_2.apply(lambda row:  row['average_nreactions'] - row['average_nreactions']/5,axis=1)

new_filtered = new_df_2[new_df_2.apply(lambda row:  row['total']>= row['cut_off'] , axis=1)]



def save_to_unfiltered_collection(df):
    unfiltered_collection = db.unfiltered_collection
    cur = unfiltered_collection.count_documents({})
    print('We had %s  entries at the start' % cur)
    
    
    id_list=list(unfiltered_collection.find({}, {"_id": 0, "id": 1}))
    id_list=list((val for dic in id_list for val in dic.values()))
    
    for dfs in df.to_dict('records'):
        if dfs['id'] not in id_list:
            unfiltered_collection.insert_one(dfs)

save_to_unfiltered_collection(new_df_2)

def save_to_filtered_collection(df):
    filtered_collection = db.filtered_collection
    cur = filtered_collection.count_documents({})
    print('We had %s  entries at the start' % cur)
    
    
    id_list=list(filtered_collection.find({}, {"_id": 0, "id": 1}))
    id_list=list((val for dic in id_list for val in dic.values()))
    
    for dfs in df.to_dict('records'):
        if dfs['id'] not in id_list:
            filtered_collection.insert_one(dfs)

save_to_filtered_collection(new_filtered)
