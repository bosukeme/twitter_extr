import datetime
import pandas as pd
from time import sleep
import boto3
import os
import json
from pymongo import MongoClient



import twitter_extraction_funcs
import extraction_service_funcs 
import article_metadata_funcs

content_bucket_name = 'bloverse-test-content'
brand_bucket_name = 'bloverse-test-brandss'

MONGO_URL = "localhost"
client = MongoClient(MONGO_URL)
db = client.new_rss_extraction

def create_folder_in_bucket(bucket_name, folder_name):
    """
    This function takes bucket name as input, as well as folder name
    and then creates a folder in that bucket
    """
    aws_access_key_id = 'AKIAXRVYY5EIKUKKLV7G'
    aws_secret_access_key = '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'

    s3 = boto3.client('s3',
                    aws_access_key_id= aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                     )
    s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))

def upload_file_to_s3(bucket_name, local_file_path, s3_file_path):
    """
    This function takes in a bucket name, local file path and s3 file path
    and uploads the content in the local file path to s3
    """
    aws_access_key_id = 'AKIAXRVYY5EIKUKKLV7G'
    aws_secret_access_key = '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'
    s3 = boto3.resource('s3',
                    aws_access_key_id= aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)    
    s3.Bucket(bucket_name).upload_file(local_file_path, s3_file_path)



def upload_to_s3(brand_id, article_details_dict, tweet_id):
    # 4b - Save the content metadata to s3
    create_folder_in_bucket(brand_bucket_name, brand_id)

    ## **AYO/Ukeme content metadata #*** To be saved to a DB table called 'Subclip Metadata' in the content service with the id being the content_id
    json_s3_file_path = '%s/%s.json' % (brand_id , tweet_id)
    temp_article_dict_path = '%s/temp.json' % os.getcwd() ## Bruno change this so that it saves to a DB
    
    with open(temp_article_dict_path, 'w') as fp:
        json.dump(article_details_dict, fp)

    # Upload the content metadata to s3
    upload_file_to_s3(brand_bucket_name, temp_article_dict_path, json_s3_file_path)




def save_to_filtered_collection(article_details_dict):
    new_rss_extraction = db.new_rss_extraction
    cur = new_rss_extraction.count_documents({})
    print('We had %s  entries at the start' % cur)
    
    
    id_list=list(new_rss_extraction.find({}, {"_id": 0, "Content ID": 1}))
    id_list=list((val for dic in id_list for val in dic.values()))
    
    #for dfs in df.to_dict('records'):
    if article_details_dict['Content ID'] not in id_list:
        new_rss_extraction.insert_one(article_details_dict)

    cur = new_rss_extraction.count_documents({})
    print('We have %s  entries at the end' % cur)


    return None




def process_tweet_urls(df_dict):
    count = 0
    for item in df_dict:
        count+=1
        print(count)
        handle = item['twitter_handle']
        url = item['urls']
        conversation_id = item['conversation_id']
        print(conversation_id)
        print(url)
    #     print(max(url))     
        print(handle)
        
        
        
        ## Split the title by "  -  " and select the first item
        if handle in  ["DisruptAfrica", "RWW", "WSJVC", "Recode", "CNET", "afrotech", "politico", "bbchealth", "bbcnews", "bbcworld", "bbctech", "BBCSport", "nytimes"]:
            try:
                brand_id = twitter_handle =  handle

                tweet_id = item['id']
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_title = article_title.split(" - ")[0]
                article_keypoints = article_summary[:2]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)
                sleep(1)

            except Exception as e:
                print(e)
            
        
        ## Split the title by " | " and select the first item.
        elif handle in ["people", "ScienceNews", "pulseghana", "indianexpress", "TechCabal", "engadget", "Nairametrics", "thetimes", "SaharaReporters"]:
            try:
                brand_id = twitter_handle =  handle

                tweet_id = item['id']

                print("===================================================================================================")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_title = article_title.split(" | ")[0]
                article_keypoints = article_summary[:2]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)
            
                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)
                sleep(1)

            except Exception as e:
                print(e)
        
        ## Split the title by " | " and select the first item..
        ## Use the second and third keypoint
        elif handle in ["WIRED", "ctvnews", "VentureBeat", "SkySportsNews", "TheBabylonBee", "NationBreaking", "givemesport"]:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_title = article_title.split(" | ")[0]
                article_keypoints = article_summary[1:3]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)
            
            
        ## Split the title by " - " and select the first item..
        ## Use the second and third keypoint
        elif handle in ['THR', "DEADLINE"]:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_title = article_title.split(" - ")[0]
                article_keypoints = article_summary[1:3]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)
                
                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
        # no issues with these
        elif handle in ["GazetteNGR", "HarvardBiz", "kenyanwalstreet", "Siftedeu", "NoCamels", "Entrepreneur", "ForbesTech", "AP_sports", "HollywoodLife"]:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[:2]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)
                
                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)
        
        
        ## title is fine
        ## use third and fourth key point
        elif handle in ['thenextweb']:
            try: 
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[2:4]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        ## Split the title by " | " and select the first item.
        ## Select the second and third keypoint
        elif handle in ['foxnews']:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[1:3]
                article_title = article_title.split(" - ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)
                
        
        
        
        ## Split the title by " – " (en dash) and select the first item
        elif handle in ['TechCrunch']:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[:2]
                article_title = article_title.split(" – ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
        
        ## Split the title by " – " (en dash) and select the first item
        ## Take the second and third keypoint
        elif handle in ['crunchbasenews']:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[1:3]
                article_title = article_title.split(" – ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
        
        ## Split the title by " – " (en dash) and select the first item.
        ## Get the THIRD AND FORTH keypoint
        elif handle in ["IndependentNGR"]:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[2:4]
                article_title = article_title.split(" – ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
        
        ## Split the title by " - " and select the first item
        ## Get the THIRD AND FORTH keypoint
        elif handle in ['verge']:
            try:
                brand_id = twitter_handle =  handle
                tweet_id = item['id']
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = article_summary[2:4]
                article_title = article_title.split(" - ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
        
        ## Split the title by "  |  " and select the first item
        ## Split the body by "Shoshanna Solomon is The Times of Israel's Startups and Business reporter " and select the second item
        elif handle in ["TOIStartup"]:
            try:
                brand_id = twitter_handle =  handle
                print("------------------------------------------------------------------------------------------------")
                print(max(url))
                article_url = max(url)
                article_title, body, article_summary = extraction_service_funcs.plan_b_article_summarisation(article_url)
                article_keypoints = [sentence.split("Shoshanna Solomon is The Times of Israel's Startups and Business reporter ") for sentence in article_summary]
                article_keypoints = [a for b in article_keypoints for a in b][1:3]
                article_title = article_title.split(" | ")[0]
                
                try:
                    article_image, conf = article_metadata_funcs.get_article_top_image(article_url)
                    print(article_image)
                except:
                    article_image = "NA"

                                
                article_details_dict = extraction_service_funcs.generate_content_details_dict_for_rss_article(article_url, article_title, article_keypoints, twitter_handle, content_bucket_name, brand_bucket_name)

                article_image_dict = {
                    "article_image": article_image
                }


                article_details_dict = {**article_details_dict, **article_image_dict}
                print(article_details_dict)

                upload_to_s3(brand_id, article_details_dict, tweet_id)
                save_to_filtered_collection(article_details_dict)

                sleep(1)
            except Exception as e:
                print(e)

        
            



def get_summary():
    """ This Commences the process"""
    start = datetime.datetime.now()

    df = twitter_extraction_funcs.run_processes()
    print("len: ", len(df))
    df_dict = df.to_dict("records")

    process_tweet_urls(df_dict)

    finish = datetime.datetime.now()

    print("the process took: ", finish - start)

get_summary()