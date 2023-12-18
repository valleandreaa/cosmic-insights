import boto3
import json
import requests
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# NASA API Key
API_KEY = "FVu0meyjKirmxLfW9mP23uPSfNfFQ01YHNPqRRng"

# S3-Buckets Name
BUCKET_NAME = "swagger23"

# Get the current date and time
current_datetime = datetime.utcnow()

def get_neo_data(page=0, sleep_duration=5):
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

def fetch_data(page_range):
    near_earth_objects = []
    get_date = (current_datetime - timedelta(days=2)).strftime("%Y-%m-%d")

    for i in page_range:
        print(f"page: {i}")
        neos = get_neo_data(i)['near_earth_objects']
        near_earth_objects.extend(last_updated_neos(neos, get_date))

    return near_earth_objects

def lambda_handler(event, context):
    # Erstellen Sie zwei Bereichssequenzen für die parallele Ausführung
    range1 = range(0, 1000)
    range2 = range(1001, 2000)

    # Verwenden Sie ThreadPoolExecutor für parallele Ausführung
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(fetch_data, [range1, range2])

    # Kombinieren Sie die Ergebnisse aus beiden Bereichen
    combined_results = []
    for result in results:
        combined_results.extend(result)

    print(f"Count number of changes: {len(combined_results)}")

    # Convert list to json
    data_string = json.dumps(combined_results, indent=2)

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
    lambda_handler(None, None)
