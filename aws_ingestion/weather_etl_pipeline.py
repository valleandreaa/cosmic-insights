import boto3
import json
import requests
import time
from datetime import datetime, timedelta

# S3-Buckets Name
BUCKET_NAME = "swagger23"

# Get the current date and time
current_datetime = datetime.utcnow()


def get_data(url, sleep_duration=5):
    try:
        with requests.Session() as session:
            # Make the request
            response = session.get(url)
            response.raise_for_status()

            # Return the JSON data if successful
            return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"For {url}: {e.response.status_code}")
        if e.response.status_code == 429:
            print(f"Too Many Requests. Waiting for {sleep_duration} seconds...")
            time.sleep(sleep_duration)
        else:
            print(f"Unexpected HTTP error. Retrying in {sleep_duration} seconds...")
            time.sleep(sleep_duration)
        print(f"Error response content: {e.response.text}")
        print("Retrying...")
        return get_data(url, sleep_duration * 2)


def process_content(specific_url):
    url = f"https://services.swpc.noaa.gov/{specific_url}"

    print(f"Processing {url}")
    page_data = get_data(url)

    print(f"Number of {specific_url} published in last 24 hours: {len(page_data)}")
    return page_data


def lambda_handler(event, context):
    all_content = {
        "mag": process_content("products/solar-wind/mag-1-day.json"),
        "plasma": process_content("products/solar-wind/plasma-1-day.json"),
        "magnetometers": process_content("json/goes/primary/magnetometers-1-day.json"),
        "protons": process_content("json/goes/primary/integral-protons-1-day.json")
    }

    # Convert list to json
    data_string = json.dumps(all_content, indent=2)

    # Create a filename with current date
    date_str = current_datetime.strftime("%Y-%m-%d")
    filename = f"weather-data-{date_str}.json"

    # Initialising S3 Client
    s3_client = boto3.client('s3')

    # Try to upload data into S3-Bucket
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
    start_time = time.time()
    lambda_handler(None, None)
    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    print(f"Execution time: {execution_time_ms} ms")
