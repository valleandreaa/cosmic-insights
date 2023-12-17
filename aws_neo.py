import boto3
import json
import requests
import time
from datetime import datetime, timedelta

# NASA API Key
API_KEY = "FVu0meyjKirmxLfW9mP23uPSfNfFQ01YHNPqRRng"

# S3-Buckets Name
BUCKET_NAME = "swagger23"

# Get the current date and time
current_datetime = datetime.utcnow()


def get_neo_data(page=0, sleep_duration=5):
    if page % 1000 == 0 and page > 0:
        print(f"Wait {61 * 60} seconds (=1h 1min) until limit resets")
        time.sleep(61 * 60)  # more than 1h
    try:
        with requests.Session() as session:
            # Make the request
            url = f"https://api.nasa.gov/neo/rest/v1/neo/browse?api_key={API_KEY}&page={page}"
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
            print("Unexpected HTTP error. Retrying in 5 seconds...")
            time.sleep(sleep_duration)
        print(f"Error response content: {e.response.text}")
        print("Retrying...")
        return get_neo_data(page, sleep_duration * 2)


def last_updated_neos(neos, today):
    return [
        neo for neo in neos if 'orbital_data' in neo and
                               'last_observation_date' in neo['orbital_data'] and
                               neo['orbital_data']['last_observation_date'] == today
    ]


def lambda_handler(event, context):
    # Due to time shift and ongoing process, we want to have all the changes from 2 days ago
    get_date = (current_datetime - timedelta(days=2)).strftime("%Y-%m-%d")
    near_earth_objects = []

    # Get total number of page
    total_page = get_neo_data()['page']['total_pages']

    # Get all asteroids which got updated
    for i in range(total_page):
        print(f"page: {i}")
        neos = get_neo_data(i)['near_earth_objects']
        near_earth_objects.extend(last_updated_neos(neos, get_date))

    print(f"Count number of changes: {len(near_earth_objects)}")

    # Convert list to json
    data_string = json.dumps(near_earth_objects, indent=2)

    # Create a filename with current date
    date_str = current_datetime.strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"nasa-data-{date_str}.json"

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
