"""
This functionality is for metadata extraction from articles
- first 3-5 sentences (based on their lengths)
- entities
- related countries
"""
import pandas as pd
import json
import random
import urllib
import uuid
import bs4
import os
import shutil
from PIL import Image
import requests
import numpy as np
from collections import Counter
from nltk import tokenize
import pickle
import wikipedia
from ast import literal_eval
import nltk
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
import re
import wikipedia
from urllib.request import urlopen
import spacy
nlp = spacy.load('en_core_web_sm')
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

import aws_s3_funcs as aws_s3
import image_utils_funcs as img_utils

"""
Add timeout functionaltiy
"""
from functools import wraps
import errno
import os
import signal

class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def process_tweet_article_url(shortened_url):
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

#@timeout(10, os.strerror(errno.ETIMEDOUT))
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


def get_article_top_image(article_url):
    """
    This function takes in an article url and then gets the headline image as well as a
    'confidence measure' on how sure we are that its actually the headline image.
    
    We will need to monitor for different publications and note any ones that seem to not
    quite work as we expected them to.
    """
    from urllib.request import urlopen
    from bs4 import BeautifulSoup
    import re

    html = urlopen(article_url)
    bs = BeautifulSoup(html, 'html.parser')
    images = bs.find_all('img', {'src':re.compile('.jpg')})

    img_mult_list = []
    img_url_list = []
    for image in images: 
        image_url = image['src']
        if 'https:' not in image_url:
            image_url = 'https:%s' % image_url
        temp_article_image_path = '%s/temp_img' % os.getcwd()
        img = img_utils.url_to_image(image_url, temp_article_image_path)
        img_mult = img.shape[0] * img.shape[1]
        img_mult_list.append(img_mult)
        img_url_list.append(image_url)

    largest_img_index = img_mult_list.index(max(img_mult_list))
    largest_image_url = img_url_list[largest_img_index]

    if largest_img_index == 0:
        confidence = 100
    else:
        confidence = int((len(img_url_list) - largest_img_index)/len(img_url_list)*100)
        
    return largest_image_url, confidence


def get_article_excerpt(article_sentences):
    """
    This gets an excerpt from an article based on the first 3-5 sentences in the article. We combine
    articles if their combined length is less than 300 characters
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
            if curr_sent_len + next_sent_len <= 300:
                excerpt_sentence_inds.append([i,i+1])
                ignore_inds.append(i+1)
            else:
                excerpt_sentence_inds.append([i])

    excerpt_inds = excerpt_sentence_inds[0:6] # We only want the first 3 sentences
#     print(excerpt_inds)
    excerpt_sentences = []
    for indices in excerpt_inds:
        sents_list = []
        for inds in indices:
            sents_list.append(article_sentences[inds])
        excerpt = ' '.join(sents_list)
#         print(excerpt)
#         print(len(excerpt))
#         print()
        excerpt_sentences.append(excerpt)
    
    return excerpt_sentences


def check_article_validity(headline, headline_image_url, article_keypoints):
    
    """
    This function processes the article details to ensure that we would actually be able to generate a video
    """
    ## Run a test here to ensure that we have sufficient details to generate the article 
    error_count = 0

    ## Headline
    # First we check the headline to ensure it meets the requirements for article generation
    if len(headline) > 2: # This means the headline isnt just some dud string - later add ability to filter out publisher names from headlines
        pass
    else:
        error_count += 1

    ## Headline Image URL
    # We pass this through the image_from_url function to ensure that it would actually work and return an image to us
    temp_article_image_path = '%s/temp_img' % os.getcwd()
    try:
        img = img_utils.url_to_image(headline_image_url, temp_article_image_path) # this means that we would be able to get the article image
    except:
        error_count += 1 

    ## Article keypoints
    for keyp in article_keypoints:
        if len(keyp) > 50:
            pass
        else:
            error_count += 1
    
    if error_count == 0:
        response = 'Pass'
    else:
        response = 'Fail'
    
    return response


