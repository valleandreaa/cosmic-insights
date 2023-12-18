import boto3
import json
import requests
from datetime import datetime, timedelta

# S3-Buckets Name
BUCKET_NAME = "swagger23"

# Get the current date and time
current_datetime = datetime.utcnow()

def get_spaceflight_data(url, content_type, sleep_duration=5):
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
        return get_spaceflight_data(url, content_type, sleep_duration * 2)

def recently_published_content(content, target_time):
    return [
        item for item in content
        if datetime.fromisoformat(item['published_at'].rstrip('Z')) > target_time
    ]

def process_content(content_type, endpoint_url):
    target_time = current_datetime - timedelta(hours=12)
    recent_content = []
    next_url = endpoint_url + "?limit=100"  # Setting limit to 100
    page_count = 0

    while next_url and page_count < 100:
        print(f"Processing {content_type}: {next_url}")
        page_data = get_spaceflight_data(next_url, content_type)
        if 'results' in page_data:
            new_content = recently_published_content(page_data['results'], target_time)
            recent_content.extend(new_content)

        next_url = page_data.get('next', None)
        page_count += 1

    print(f"Number of {content_type} published in last 12 hours: {len(recent_content)}")
    return recent_content

def lambda_handler(event, context):
    all_content = {
        "articles": process_content("articles", "https://api.spaceflightnewsapi.net/v4/articles"),
        "blogs": process_content("blogs", "https://api.spaceflightnewsapi.net/v4/blogs"),
        "reports": process_content("reports", "https://api.spaceflightnewsapi.net/v4/reports")
    }

    data_string = json.dumps(all_content, indent=2)
    filename = f"spaceflight-content-{current_datetime.strftime('%Y-%m-%dT%H-%M-%SZ')}.json"

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

