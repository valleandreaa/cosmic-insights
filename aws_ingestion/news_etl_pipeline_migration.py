import boto3
import json
import requests
from datetime import datetime
import time

# S3-Buckets Name
BUCKET_NAME = "swagger23"

def get_spaceflight_data(url, sleep_duration=5):
    try:
        with requests.Session() as session:
            response = session.get(url)
            response.raise_for_status()

            return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"For {url}: {e.response.status_code}")
        if e.response.status_code == 429:
            print(f"Too Many Requests. Waiting for {sleep_duration} seconds...")
            time.sleep(sleep_duration)
        else:
            print("Unexpected HTTP error. Retrying in 5 seconds...")
            time.sleep(sleep_duration)
        print(f"Error response content: {e.response.text}")
        print("Retrying...")
        return get_spaceflight_data(url, sleep_duration * 2)

def process_content(content_type, endpoint_url):
    all_content = []
    next_url = endpoint_url + "?limit=100"
    
    while next_url:
        print(f"Processing {content_type}: {next_url}")
        page_data = get_spaceflight_data(next_url)
        if 'results' in page_data:
            all_content.extend(page_data['results'])

        next_url = page_data.get('next', None)

    print(f"Total number of {content_type} collected: {len(all_content)}")
    return all_content

def lambda_handler(event, context):
    combined_content = {
        "articles": process_content("articles", "https://api.spaceflightnewsapi.net/v4/articles"),
        "blogs": process_content("blogs", "https://api.spaceflightnewsapi.net/v4/blogs"),
        "reports": process_content("reports", "https://api.spaceflightnewsapi.net/v4/reports")
    }

    data_string = json.dumps(combined_content, indent=2)
    filename = f"spaceflight-content-{datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')}.json"

    # Initialising S3 Client
    s3_client = boto3.client('s3')

    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=filename, Body=data_string)
        print(f"Successfully uploaded {filename} to {BUCKET_NAME}")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully uploaded {filename} to {BUCKET_NAME}")
        }
    except Exception as e:
        print(f"Error uploading the file: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps("Error uploading the file")
        }

# Test the function
if __name__ == "__main__":
    lambda_handler(None, None)

