import pandas as pd
import requests
import torch
import torch.nn as nn
from sklearn.preprocessing import StandardScaler

# Assuming NeuralNetwork is defined in neural_network.py
from neural_network import NeuralNetwork
import joblib
import torch


def fetch_neo_data():
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?page=1&size=10&api_key=DEMO_KEY"
    response = requests.get(url)
    if response.status_code == 200:
        fetch_data = response.json()
        return fetch_data['near_earth_objects']
    else:
        print("Failed to retrieve data from the API")
        return None

# Function to extract relevant data for inference
def extract_data_for_inference(neo_data):
    extracted_data_list = []
    for asteroid in neo_data:
        orbital_data = asteroid['orbital_data']
        eccentricity = orbital_data.get('eccentricity', None)
        semi_major_axis = orbital_data.get('semi_major_axis', None)
        inclination = orbital_data.get('inclination', None)
        velocity = float(orbital_data.get('perihelion_distance', 0)) * float(orbital_data.get('mean_motion', 0))
        closest_approach_data = asteroid.get('close_approach_data', [])
        distance_from_earth = float(closest_approach_data[0]['miss_distance'].get('kilometers', 0)) if closest_approach_data else 0

        extracted_data_list.append({
            'eccentricity': eccentricity,
            'semi_major_axis': semi_major_axis,
            'inclination': inclination,
            'velocity': velocity,
            'distance_from_earth': distance_from_earth
        })

    return pd.DataFrame(extracted_data_list)

# Function to load the trained PyTorch model
def load_model(model_path):
    model = NeuralNetwork(5)
    model.load_state_dict(torch.load(model_path))
    model.eval()
    return model

# Function to prepare data for inference
def prepare_data_for_inference(scaler, new_data):
    scaled_data = scaler.transform(new_data)
    return torch.tensor(scaled_data, dtype=torch.float32)

# Function to perform inference
def infer_and_label(model, data_tensor):
    with torch.no_grad():
        predictions = model(data_tensor)
        binary_predictions = (predictions > 0.5).float().numpy()
        labels = ["Threat" if pred == 1 else "Non-Threat" for pred in binary_predictions]
        return labels


# Main execution
if __name__ == "__main__":
    model = load_model('threat_model.pt')

    # Load the scaler
    scaler = joblib.load('scaler.pkl')

    # Fetch and extract new data for inference
    new_data = fetch_neo_data()
    if new_data:
        prepared_data = extract_data_for_inference(new_data)
        data_tensor = prepare_data_for_inference(scaler, prepared_data)

        # Run inference and get labeled predictions
        labeled_predictions = infer_and_label(model, data_tensor)
        print("Predictions:", labeled_predictions)
    else:
        print("No data available for inference.")