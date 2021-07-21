"""
These are functions for taking a twitter handle and tweet ID and then generate a brand_details_dict and article_details_dict
"""
import os
import sys  
import json
import uuid
import requests
from datetime import datetime, timedelta
import random
import twint
import nest_asyncio
import pandas as pd
nest_asyncio.apply()
import spacy
nlp = spacy.load('en_core_web_sm')
import bs4
from bs4 import BeautifulSoup
from bs4.element import Comment
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

#sys.path.insert(0, '/Users/dr_d3mz/Documents/GitHub/Bloverse Video Engine/General Functions') # insert the path to your functions folder

import aws_s3_funcs as aws_s3
import ibm_watson_nlp_funcs as ibm_nlp
import image_utils_funcs as img_utils
import text_utils_funcs as txt_utils
import article_metadata_funcs as art_meta
import audio_utils_funcs as aud_utils

"""
Twitter
"""
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


def get_twitter_handle_bio_details(twitter_handle):
    try:
        c = twint.Config()
        c.Username = twitter_handle
        c.Store_object = True
        c.User_full = False
        c.Pandas =True
        c.Hide_output = True

        twint.run.Lookup(c)
        user_df = twint.storage.panda.User_df.drop_duplicates(subset=['id'])

        try:
            user_id = list(user_df['id'])[0]
        except:
            user_id = 'NA'

        try:
            user_name = list(user_df['name'])[0]
        except:
            user_name = 'NA'

        try:
            user_bio = list(user_df['bio'])[0]
        except:
            user_bio = 'NA'

        try:
            user_profile_image_url = list(user_df['avatar'])[0]
        except:
            user_profile_image_url = 'NA'

        try:
            user_url = list(user_df['url'])[0]
        except:
            user_url = 'NA'

        try:
            user_join_date = list(user_df['join_date'])[0]
        except:
            user_join_date = 'NA'

        try:
            user_location = list(user_df['location'])[0]
        except:
            user_location = 'NA'

        try:
            user_following = list(user_df['following'])[0]
        except:
            user_following = 'NA'

        try:
            user_followers = list(user_df['followers'])[0]
        except:
            user_followers = 'NA'

        try:
            user_verified = list(user_df['verified'])[0]
        except:
            user_verified = 'NA'

    except:
        user_name = 'NA'
        user_bio = 'NA'
        user_profile_image_url = 'NA'
        user_url = 'NA'
        user_join_date = 'NA'
        user_location = 'NA'
        user_following = 'NA'
        user_followers = 'NA'
        user_verified = 'NA'
    
    return twitter_handle, user_name, user_bio, user_profile_image_url, user_url, user_location, user_following, user_followers, user_verified

"""
Summarisation
"""
import requests
def process_summary_from_url(article_url, tldr_key):
    try:
        tldr_url = "https://tldrthis.p.rapidapi.com/v1/model/abstractive/summarize-url/"
        payload = f'{{"url":"{article_url}", "min_length": 100, "max_length": 250, "is_detailed": false}}'
        headers = {
            'content-type': "application/json",
            'x-rapidapi-key': tldr_key, #change this
            'x-rapidapi-host': "tldrthis.p.rapidapi.com"
            }
        response = requests.request("POST", tldr_url, data=payload, headers=headers) #.text
#         print(response)
    except Exception as e:
        print(e)
        response = 'NA'
        pass
    
    return response

def get_p_tags_from_link(soup):
    """
    This function extracts paragraph tags from the article HTML info
    """
    # get text
    paragraphs = soup.find_all(['p', 'strong', 'em'])

    txt_list = []
    tag_list = []
    
    for p in paragraphs:
        if p.href:
            pass
        else:
            if len(p.get_text()) > 20: # this filters out things that are most likely not part of the core article
                tag_list.append(p.name)
                txt_list.append(p.get_text())

    ## This snippet of code deals with duplicate outputs from the html, helps us clean up the data further
    txt_list2 = []
    for txt in txt_list:
        if txt not in txt_list2:
            txt_list2.append(txt)
    
    return txt_list2

def get_domain_url_from_article_url(article_url):
    """
    This function takes in the article url and gets the domain
    """
    import tldextract
    extracted_domain = tldextract.extract(article_url)
    domain = "{}.{}".format(extracted_domain.domain, extracted_domain.suffix)
    domain_url = 'www.%s' % domain
    return domain_url


def get_article_title_and_body(article_url):
    """
    This function takes an article url then gets the title and body
    """
    response = requests.get(article_url, headers=headers)
    soup = bs4.BeautifulSoup(response.text,'lxml')

    # Get the article title
    title = soup.find(['h1','title']).get_text()
    #     print(title)
    article_text_sents = get_p_tags_from_link(soup)
    body = ' '.join(article_text_sents)

    return title, body

def get_article_excerpt(article_sentences):
    """
    This gets an excerpt from an article based on the first 3-5 sentences in the article. We combine
    articles if their combined length is less than 250 characters
    """
    ### This part can be removed later as its a bit redundant but the algo is fast enough for this not to be an issue
    # Get the p tags across the article

    excerpt_sentence_inds = [] # these are the start indices for the 3 'keypoints' that we will select to make the excerpt of the article
    ignore_inds = [] # sentences that are going to be ignored
    for i in range(len(article_sentences)):
        if i not in ignore_inds and i < 6:
            sentence = article_sentences[i]
            curr_sent_len = len(sentence)
            next_sent_len = len(article_sentences[i+1])
            if curr_sent_len + next_sent_len <= 250:
                excerpt_sentence_inds.append([i,i+1])
                ignore_inds.append(i+1)
            else:
                excerpt_sentence_inds.append([i])

    excerpt_inds = excerpt_sentence_inds[0:6] # We only want the first 2 sentences
    excerpt_sentences = []
    for indices in excerpt_inds:
        sents_list = []
        for inds in indices:
            sents_list.append(article_sentences[inds])
        excerpt = ' '.join(sents_list)
        excerpt_sentences.append(excerpt)
    
    return excerpt_sentences


def plan_b_article_summarisation(article_url):
    """
    This is the article summarisation that we use in the event where tldrthis fails
    """
    title, body = get_article_title_and_body(article_url)

    doc = nlp(body)
    summary_sentences = []
    for sent in doc.sents:
        summary_sentences.append(sent.text.strip())

    summary_sentences = [sent for sent in summary_sentences if len(sent) > 50]

    article_summary = get_article_excerpt(summary_sentences)

    return title, body, article_summary

def get_article_summary(article_url, tldr_key):
    """
    This function gets an article url, then processes it to get the 'summary', 'article_text', 'article_title'
    """
    article_meta = process_summary_from_url(article_url, tldr_key)

    # Process the article meta to get summary etc etc
    output_dict = article_meta.json()
    
    try:
        summary = output_dict['summary'][0]
        article_text = output_dict['article_text']
        article_title = output_dict['article_title']
        
        # Process the summary and split into keypoints
        doc = nlp(summary)
        summary_sentences = []
        for sent in doc.sents:
            summary_sentences.append(sent.text.strip())

        excerpt_sentence_inds = [] # these are the start indices for the 3 'keypoints' that we will select to make the excerpt of the article
        ignore_inds = [] # sentences that are going to be ignored
        num_keyp = 0
        for i in range(len(summary_sentences)):

            if i not in ignore_inds and i < 6:
                sentence = summary_sentences[i]
                curr_sent_len = len(sentence)
                try:
                    next_sent_len = len(summary_sentences[i+1])
                    if curr_sent_len + next_sent_len <= 250:
                        excerpt_sentence_inds.append([i,i+1])
                        ignore_inds.append(i+1)
                        num_keyp += 1
                    else:
                        excerpt_sentence_inds.append([i])
                        num_keyp += 1
                except:
                    if num_keyp < 4:
                        excerpt_sentence_inds.append([i])
                    pass

        excerpt_inds = excerpt_sentence_inds[0:2] # We only want the first 2 sentences

        article_keypoints = []
        for indices in excerpt_inds:
            sents_list = []
            for inds in indices:
                sents_list.append(summary_sentences[inds])
            excerpt = ' '.join(sents_list)
            article_keypoints.append(excerpt)
    except:
        # Add functionality that does this the old school way using requests, bs4 etc etc
        article_title, article_text, article_keypoints = plan_b_article_summarisation(article_url)
    


    return article_title, article_text, article_keypoints

"""
Content/Brand Generation
"""
def generate_content_details_dict_for_rss_article(article_url, article_title,  article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name):
    """
    Step 1 - Get the general article details
    """
    ### You should create an article dict here that would encapsulate the article details like background music etc, as well as the headline and keypoint details
    ## Background music
    background_audio_list = ['Angry', 'Calm', 'Cheerful', 'Hopeful', 'Reflective', 'Sad'] # ok keep this in for now
    rand_num = random.randint(0,len(background_audio_list)-1)
    article_mood = background_audio_list[rand_num] ## This should randomly choose from the available options for now
    
    ## AI Voice
    # Generate the azure voice_name_dict
    azure_voice_name_dict = aud_utils.generate_azure_voice_name_dict()
    voice_names_list = list(azure_voice_name_dict.keys())
    rand_num = random.randint(0,len(voice_names_list)-1)
    ai_voice_name = voice_names_list[rand_num] ## This should randomly choose from the available options for now
    
    background_music_url = 'NA' # this would be a dict with the bucket name, and file name of where the background music is
    article_voiceover_type = 'AI Voice'
    article_font_name = 'OpenSans-ExtraBold.ttf' #** Something we should def do later is find very solid alternate fonts to use
    article_primary_colour = '#550afb'
    article_secondary_colour = '#ffffff'

    """
    Step 2 - Get the headline details
    """
    headline = article_title

    headline_dict = {
        'Headline Text': headline,
    }

    """
    Step 3 - Generate the keypoint dict
    """

    ## This is just fluff code to create the dict, but John should actually br producing a dict for us as input
    article_keypoints_dict = {}
    for i in range(len(article_keypoints)):
        ind = i+1
        name = 'keypoint_%s' % ind
        keypoint_text = article_keypoints[i]
        keypoint_dict = {
            'Keypoint Text': keypoint_text,
        }
        # Update the dict
        d1 = {name:keypoint_dict.copy()}
        article_keypoints_dict.update(d1) 


    article_details_dict = {
        'Content Type': 'RSS Article',
        'Content Bucket Name': content_bucket_name,
        'Brand Bucket Name': brand_bucket_name,
        'Content ID': str(uuid.uuid4()),
        'Brand ID' : twitter_handle, # actually for the brand_id, we could just try to use the twitter handle as this would likely be unique... something to ponder
        'Article Mood': article_mood,
        'Article Font Name': article_font_name,
        'Article URL' : article_url,
        'Article Voiceover Type' : 'AI Voice',
        'AI Voice Name' : ai_voice_name,
        'Article Headline': headline_dict, # For each brand we create a randomly generated ID for the brand
        'Article Keypoints': article_keypoints_dict,
        'Article Primary Colour': article_primary_colour,
        'Article Secondary Colour': article_secondary_colour 
    }
    
    return article_details_dict



def generate_brand_details_dict_from_twitter_handle(twitter_handle, user_name, brand_url, brand_logo_url, brand_bucket_name):
    
    brand_url_text = 'For more on this post visit us at %s' % brand_url
    brand_twitter_handle = twitter_handle
    brand_font_name = 'OpenSans-ExtraBold.ttf'
    brand_primary_colour, brand_secondary_colour = img_utils.determine_brand_colours(brand_logo_url)
    
    brand_details_dict = {
        'Brand Bucket Name' : brand_bucket_name,
        'Brand ID' : str(uuid.uuid4()),
        'Brand Name': user_name, # For each brand we create a randomly generated ID for the brand
        'Brand URL': brand_url,
        'Brand URL Text': brand_url_text,
        'Brand Twitter Handle': twitter_handle,
        'Brand Logo URL': brand_logo_url,
        'Brand Font Name': brand_font_name,
        'Brand Primary Colour' : brand_primary_colour,
        'Brand Secondary Colour': brand_secondary_colour
    }
    return brand_details_dict


def process_shortened_article_url(shortened_url):
    """
    This function takes a shortened URL from twitter etc and then unfurls it so that we have
    the actual base url
    """
    import urlexpander
    import requests
    
    try:
        article_url = urlexpander.expand(shortened_url)
        if 'CLIENT_ERROR' in article_url:
            response = requests.get(shortened_url)
            article_url = response.url
            
        ## If we identify where there's a '?' we can filter out 
        try:
            break_index = article_url.index('?') # this identifies the break index for the article and then allows us to filter out everything after that so we have the raw article
            if break_index > 0:
                article_url = article_url[0:article_url.index('?')]
        except:
            pass
    except Exception as e:
        print(e)
        article_url = 'NA'
    return article_url


def process_article_and_brand_details_from_tweet_id(twitter_handle, article_tweet_id, tldr_key, content_bucket_name, brand_bucket_name):
    """
    This function takes a twitter handle, tweet ID and then processes them to generate article and brand details
    """
    # 1 -  Get the data for your search parameters
    search_dt = datetime.now() - timedelta(3)
    search_date = datetime.strftime(search_dt, '%Y-%m-%d')
    num_tweets = 5000 
    
    # 2 - Get the article tweet details
    tweet_df = get_latest_tweets_from_handle(twitter_handle, num_tweets, search_date)
    article_tweet_df = tweet_df[tweet_df['id']==article_tweet_id]
    
    # 3 - Process the article tweet to get its metadata
    article_url = article_tweet_df['urls'].iloc[0][0]
#     print(article_url)
    # 3b - Process the url in the case where its shortened
    article_url = process_shortened_article_url(article_url)
#     print(article_url)
    article_title, article_text, article_keypoints = get_article_summary(article_url, tldr_key)
    
    # 4 - Generate the content details dict
    content_details_dict = generate_content_details_dict_for_rss_article(article_url, article_title, article_text, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)
    content_id = content_details_dict['Content ID']
    
    # 4b - Save the content metadata to s3
    aws_s3.create_folder_in_bucket(content_bucket_name, content_id)

    ## **AYO/Ukeme content metadata #*** To be saved to a DB table called 'Subclip Metadata' in the content service with the id being the content_id
    json_s3_file_path = '%s/content_details.json' % content_id
    temp_content_dict_path = '%s/temp.json' % os.getcwd() ## Bruno change this so that it saves to a DB
    with open(temp_content_dict_path, 'w') as fp:
        json.dump(content_details_dict, fp)
    
    # Upload the content metadata to s3
    aws_s3.upload_file_to_s3(content_bucket_name, temp_content_dict_path, json_s3_file_path)
    
    ## Process the twitter handle details to generate the brand details dict
    user_name, user_bio, user_profile_image_url, user_url, user_location, user_following, user_followers, user_verified = get_twitter_handle_bio_details(twitter_handle)
    brand_logo_url = user_profile_image_url
    brand_url = art_meta.get_domain_url_from_article_url(article_url)

    # Generate the brand details dict
    brand_details_dict = generate_brand_details_dict_from_twitter_handle(twitter_handle, user_name, brand_url, brand_logo_url, brand_bucket_name)
    brand_id = twitter_handle
    
    # 4b - Save the content metadata to s3
    aws_s3.create_folder_in_bucket(brand_bucket_name, brand_id)

    ## **AYO/Ukeme content metadata #*** To be saved to a DB table called 'Subclip Metadata' in the content service with the id being the content_id
    json_s3_file_path = '%s/brand_details.json' % brand_id
    temp_brand_dict_path = '%s/temp.json' % os.getcwd() ## Bruno change this so that it saves to a DB
    with open(temp_brand_dict_path, 'w') as fp:
        json.dump(brand_details_dict, fp)
    
    # Upload the content metadata to s3
    aws_s3.upload_file_to_s3(brand_bucket_name, temp_brand_dict_path, json_s3_file_path)
    
    return content_id, brand_id


"""
Article Metadata - (Later add provision for dealing with comments, and then stories)
"""

def get_article_top_3_entities(entity_dict):
    """
    This function takes in the entity dict and then returns the top 3 poc entities within the body of text
    """
    processable_entity_types = ['Person', 'Broadcaster', 'Company', 'Facility', 'HealthCondition', 'JobTitle', 'Movie', 'MusicGroup', 'NaturalEvent'
                           'Organization', 'PrintMedia', 'Sport', 'SportingEvent', 'TelevisionShow', 'Vehicle', 'Anatomy', 'Award', 'GeographicFeature',
                               'Location']
    
    count = 0
    article_top_3_entities = []

    for ent in entity_dict:
        ent_dict = entity_dict[ent]
        entity_type = ent_dict['Entity Type']
        if entity_type in processable_entity_types:
            article_top_3_entities.append(ent)
            count +=1
            if count > 2:
                break

    return article_top_3_entities


def get_article_text_metadata(article_text): # This will be superceded by what Bruno has done, but we will add an extra field for the top_3_poc_entities
    """
    Get text metadata dict - add the extra field for top_3_poc_entities to what Bruno already had in the analysis service
    """
    ## Get Article categories and process
    category_tags, category_dict = ibm_nlp.generate_text_categories(article_text)

    ## Get Article keywords and process
    keyword_tags, keyword_dict = ibm_nlp.generate_text_keywords(article_text)

    ## Get Article concepts and process
    concept_tags, concept_dict = ibm_nlp.generate_text_concepts(article_text)

    ## Get Article entities and process
    entity_tags, entity_dict = ibm_nlp.generate_text_entities(article_text)

    ## Get Article sentiment and process
    article_sentiment, article_sentiment_score = ibm_nlp.generate_text_sentiment(article_text)

    article_top_3_entities = get_article_top_3_entities(entity_dict)

    text_metadata_dict = {
        'Category Tags' : category_tags,
        'Category Dict' : category_dict,
        'Keyword Tags' : keyword_tags,
        'Keyword Dict' : keyword_dict,
        'Concept Tags' : concept_tags,
        'Concept Dict' : concept_dict,
        'Entity Tags' : entity_tags,
        'Entity Dict' : entity_dict,
        'Top 3 Article Entities' : article_top_3_entities,
        'Article Sentiment' : article_sentiment,
        'Article Sentiment Score' : article_sentiment_score,
    }
    
    return text_metadata_dict


