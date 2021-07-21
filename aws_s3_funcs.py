import boto3 

def get_bucket_folder_names(bucket_name):
    """
    This functions gets a list of all available user folders on the s3 bucket
    """
    
    s3 = boto3.client("s3")
    all_objects = s3.list_objects(Bucket = bucket_name) 
    
    folder_list = []
    for obj in all_objects['Contents']:
#         print(obj)
        folder_list.append(obj['Key'])
        
    return folder_list

def create_folder_in_bucket(bucket_name, folder_name):
    """
    This function takes bucket name as input, as well as folder name
    and then creates a folder in that bucket
    """
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))
    
    
def upload_file_to_s3(bucket_name, local_file_path, s3_file_path):
    """
    This function takes in a bucket name, local file path and s3 file path
    and uploads the content in the local file path to s3
    """
    s3 = boto3.resource('s3')    
    s3.Bucket(bucket_name).upload_file(local_file_path, s3_file_path)
    
    
def download_s3_file(bucket_name, folder_name, file_name, local_output_path):
    """
    This function takes a bucket name, folder name, file name and local output path and
    downloads the content from s3 and saves to that local output path
    """
    s3_file_key = '%s/%s' % (folder_name, file_name)
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, s3_file_key, local_output_path)
    

def download_twitter_trending_tags(trending_bucket_name, file_name, local_output_path):
    """
    This function takes a bucket name, folder name, file name and local output path and
    downloads the content from s3 and saves to that local output path
    """
    s3 = boto3.client('s3')
    s3.download_file(trending_bucket_name, file_name, local_output_path)