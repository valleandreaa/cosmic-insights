from datetime import datetime
import json
import requests
import time

# NASA API Key
API_KEY = "FVu0meyjKirmxLfW9mP23uPSfNfFQ01YHNPqRRng"

# Get the current date and time
current_datetime = datetime.utcnow()


def get_data(page=0, sleep_duration=5):
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
        return get_data(page, sleep_duration * 2)


def last_updated_neos(neos):
    return [neo for neo in neos if 'orbital_data' in neo]


def lambda_handler(event, context):
    near_earth_objects = []

    # Get total number of page
    total_page = get_data()['page']['total_pages']

    # Get all asteroids which got updated
    for i in range(total_page):
        print(f"page: {i}")
        neos = get_data(i)['near_earth_objects']
        near_earth_objects.extend(last_updated_neos(neos))

    print(f"Count number of changes: {len(near_earth_objects)}")

    # Convert list to json
    data_string = json.dumps(near_earth_objects, indent=2)

    # Create a filename with current date
    date_str = current_datetime.strftime("%Y-%m-%d")
    filename = f"neo-data-all-{date_str}.json"

    # Save JSON data to a file
    with open(filename, "w") as json_file:
        json_file.write(data_string)

    print(f"JSON data saved to {filename}")


# Test the function
if __name__ == "__main__":
    start_time = time.time()
    lambda_handler(None, None)
    end_time = time.time()
    execution_time_ms = (end_time - start_time) * 1000
    print(f"Execution time: {execution_time_ms} ms")
