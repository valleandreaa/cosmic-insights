import requests
import boto3
import json
from datetime import datetime

# Fetcher for the API
def fetcher(data):
    asteroids = {
        'id': int(data['id']),
        'name': data['name'],
        'nasa_jpl_url': data['nasa_jpl_url'],
        'absolute_magnitude_h': data['absolute_magnitude_h'],
        'estimated_diameter': data['estimated_diameter'],
        'is_potentially_hazardous_asteroid': data['is_potentially_hazardous_asteroid'],
        'close_approach_data': data['close_approach_data'],  
        'orbital_data': data['orbital_data'],
        'is_sentry_object': data['is_sentry_object']
    }
    return asteroids

def lambda_handler(event, context):
    # Ihr API-Schlüssel für die NASA-API
    api_key = "FVu0meyjKirmxLfW9mP23uPSfNfFQ01YHNPqRRng"
    near_earth_objects = []

    # Anzahl der abzurufenden Seiten
    number_batches = 10

    # Daten von der NASA API abrufen
    for i in range(number_batches):
        url = f"https://api.nasa.gov/neo/rest/v1/neo/browse?page={i}&size=20&api_key={api_key}"
        r = requests.get(url)
        data = r.json()
        near_earth_objects.extend(data['near_earth_objects'])

    # Verarbeiten Sie die abgerufenen Daten
    list_asteroids = [fetcher(obj) for obj in near_earth_objects]

    # Der Name Ihres S3-Buckets
    BUCKET_NAME = "swagger11"

    # Daten in einen String umwandeln (JSON-Format)
    data_string = json.dumps(list_asteroids)

    # Erzeugen Sie einen Dateinamen mit dem aktuellen Datum und Uhrzeit
    date_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"nasa-data-{date_str}.json"

    # Initialisieren Sie einen S3 Client
    s3_client = boto3.client('s3')

    # Versuchen Sie, die Daten in den S3-Bucket hochzuladen
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
    print(lambda_handler(None, None))
