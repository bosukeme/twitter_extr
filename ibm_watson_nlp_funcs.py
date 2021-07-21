"""
These functions are all about how we leverage IBM Watson to enrich our content
"""
import json
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, CategoriesOptions, KeywordsOptions, ConceptsOptions, EntitiesOptions, SentimentOptions

authenticator = IAMAuthenticator('FvNE4IXDXlY9V1LShzev2V6s-azux6C3wA3KMncwIRLA')
natural_language_understanding = NaturalLanguageUnderstandingV1(
    version='2020-08-01',
    authenticator=authenticator
)

natural_language_understanding.set_service_url('https://api.eu-gb.natural-language-understanding.watson.cloud.ibm.com/instances/09b32c4f-017c-4d0f-931f-ed6dfc20cdec')


def generate_text_categories(text):
    """
    This function gets the top 5 categories 
    """
    # Make API call to IBM Watson
    response = natural_language_understanding.analyze(
        text= text,
        features=Features(categories=CategoriesOptions(limit=5))).get_result()

    # Process the category response
    article_category_dict = {}
    category_response = response['categories']
    category_tags = []
    
    for cat in category_response:
        conf_score = cat['score']
        category_name = cat['label']
        category_levels = category_name.split('/')
        category_levels = [cat for cat in category_levels if len(cat) > 0]
        category_tags += category_levels

        cat_dict = {
            'Name': category_name,
            'Confidence Score': conf_score
        }
        article_category_dict.update({category_name:cat_dict})
    
    return category_tags, article_category_dict


def generate_text_keywords(text):
    
    response = natural_language_understanding.analyze(
        text= text,
        features=Features(keywords=KeywordsOptions(sentiment=True,emotion=True,limit=10))).get_result()

    # Process the keyword response
    keyword_response = response['keywords']
            
    keyword_tags = []
    keywords_dict = {}
    for key in keyword_response:
        keyword = key['text']
        keyword_tags.append(keyword)
        sentiment_label = key['sentiment']['label']
        sentiment_score = key['sentiment']['score']
        relevance_score = key['relevance']
        emotion_score_dict = key['emotion']
        occurence_count = key['count']
        
        curr_dict = {
            'Sentiment' : sentiment_label,
            'Sentiment Score' : sentiment_score,
            'Keyword Relevance Score' : relevance_score, 
            'Keyword Emotion Dict' : emotion_score_dict,
            'Keyword Occurence' : occurence_count   
        }
        keywords_dict.update({keyword:curr_dict})
        
    return keyword_tags, keywords_dict


def generate_text_concepts(text):

    response = natural_language_understanding.analyze(
        text= text,
        features=Features(concepts=ConceptsOptions(limit=10))).get_result()

    # Process the concept response
    concept_response = response['concepts']
            
    concept_tags = []
    concept_dict = {}
    
    for con in concept_response:
        
        concept_name = con['text']
        relevance_score = con['relevance']
        dbpedia_link = con['dbpedia_resource']
        
        curr_concept = {
            'Relevance Score' : relevance_score,
            'DBPedia Link' : dbpedia_link
        }
        
        concept_dict.update({concept_name:curr_concept})
        concept_tags.append(concept_name)
    
    return concept_tags, concept_dict


def generate_text_entities(text):
    response = natural_language_understanding.analyze(
        text= text,
        features=Features(entities=EntitiesOptions(sentiment=True,limit=10))).get_result()

    # Process the entity response
    entities_response = response['entities']
            
    entity_tags = []
    entity_dict = {}
    for ent in entities_response:
        entity = ent['text']
        entity_tags.append(entity)
        entity_type = ent['type']
        sentiment_label = ent['sentiment']['label']
        sentiment_score = ent['sentiment']['score']
        relevance_score = ent['relevance']
        confidence_score = ent['confidence']
        occurence_count = ent['count']
        try:
            disambiguation = ent['disambiguation']
        except:
            disambiguation = 'NA'
            
        try:
            dbpedia = ent['dbpedia_resource']
        except:
            dbpedia = 'NA'
            
        curr_dict = {
            'Entity Type' : entity_type,
            'Entity Subtypes' : disambiguation,
            'Sentiment' : sentiment_label,
            'Sentiment Score' : sentiment_score,
            'Entity Relevance Score' : relevance_score, 
            'Entity Confidence Score' : confidence_score,
            'Entity Occurence' : occurence_count,
            'DBPedia Resource' : dbpedia
        }
        entity_dict.update({entity:curr_dict})
        
    return entity_tags, entity_dict


def generate_text_sentiment(text):
    response = natural_language_understanding.analyze(
        text= text,
        features=Features(sentiment=SentimentOptions())).get_result()

    # Process the sentiment response
    sentiment_response = response['sentiment']
    text_sentiment = sentiment_response['document']['label']
    text_sentiment_score = sentiment_response['document']['score']
    
    return text_sentiment, text_sentiment_score


def get_metadata_for_text(text):
    # Get categories
    category_tags, category_dict = generate_text_categories(text)

    # Get keywords
    keyword_tags, keywords_dict = generate_text_keywords(text)

    # Get Concepts
    concept_tags, concepts_dict = generate_text_concepts(text)

    # Get Entities
    entity_tags, entity_dict = generate_text_entities(text)

    # Get Sentiment
    text_sentiment, text_sentiment_score = generate_text_sentiment(text)

    text_metadata  = {
            'Category Tags' : category_tags,
            'Category Dict' : category_dict,
            'Keyword Tags' : keyword_tags,
            'Keyword Dict' : keywords_dict,
            'Concept Tags' : concept_tags,
            'Concept Dict' : concepts_dict,
            'Entity Tags' : entity_tags,
            'Entity Dict' : entity_dict,
            'Sentiment' : text_sentiment,
            'Sentiment Score' : text_sentiment_score,
        }

    return text_metadata