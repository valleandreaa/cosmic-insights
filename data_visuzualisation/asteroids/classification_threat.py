import pandas as pd
import requests
import torch
import torch.nn as nn
import torch.optim as optim
from neural_network import NeuralNetwork
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


# Function to fetch NEO data
def fetch_neo_data():
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?page=1&size=10&api_key=U1rN3FyZdsp9dpqaKIFRRAl92EtaBV9AUGdb8lei"
    response = requests.get(url)
    if response.status_code == 200:
        fetch_data = response.json()
        return fetch_data['near_earth_objects']
    else:
        print("Failed to retrieve data from the API")
        return None


def extract_data(neo_data):
    # Create DataFrame from fetched data
    extracted_data_list = []
    for asteroid in neo_data:
        # Extract relevant parameters and threat level (if available) from the API
        orbital_data = asteroid['orbital_data']
        eccentricity = orbital_data.get('eccentricity', None)
        semi_major_axis = orbital_data.get('semi_major_axis', None)
        inclination = orbital_data.get('inclination', None)

        # Extracting velocity from the orbit (example based on your initial script)
        velocity = float(orbital_data.get('perihelion_distance', 0)) * float(orbital_data.get('mean_motion', 0))

        # Extracting distance from Earth from the closest approach data
        closest_approach_data = asteroid.get('close_approach_data', [])
        distance_from_earth = float(
            closest_approach_data[0]['miss_distance'].get('kilometers', 0)) if closest_approach_data else 0

        # Assuming you have some criteria for defining threat level (this is just an example)
        threat_level = 1 if asteroid['is_potentially_hazardous_asteroid'] else 0

        # Append the extracted data to the list
        extracted_data_list.append({
            'eccentricity': eccentricity,
            'semi_major_axis': semi_major_axis,
            'inclination': inclination,
            'velocity': velocity,
            'distance_from_earth': distance_from_earth,
            'threat_level': threat_level
        })

    # Create DataFrame from the extracted data
    extracted_data = pd.DataFrame(extracted_data_list)

    # Drop rows with missing values
    extracted_data.dropna(inplace=True)
    return extracted_data


def prepare_data(df_data):
    # Split data into features and target variable
    x = df_data.drop('threat_level', axis=1)
    y = df_data['threat_level']

    # Split the data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # Normalize features using StandardScaler
    scaler = StandardScaler()
    x_train = scaler.fit_transform(x_train)
    x_test = scaler.transform(x_test)

    # Convert data to PyTorch tensors
    x_train_tensor = torch.tensor(x_train, dtype=torch.float32)
    x_test_tensor = torch.tensor(x_test, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)
    y_test_tensor = torch.tensor(y_test.values, dtype=torch.float32).view(-1, 1)

    return x_train.shape[1], x_train_tensor, x_test_tensor, y_train_tensor, y_test_tensor


def train_model(base_model, x_train, y_train):
    # Define loss function and optimizer
    criterion = nn.BCELoss()  # Binary Cross Entropy Loss
    optimizer = optim.Adam(base_model.parameters(), lr=0.001)

    # Training the model
    num_epochs = 1000
    for epoch in range(num_epochs):
        optimizer.zero_grad()
        outputs = base_model(x_train)
        loss = criterion(outputs, y_train)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 100 == 0:
            print(f'Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}')

    torch.save(model.state_dict(), 'threat_model.pt')


if __name__ == "__main__":
    # Fetch NEO data
    data = fetch_neo_data()

    # Get the data
    df_asteroids = extract_data(data)
    input_size, x_train_data, x_test_data, y_train_data, y_test_data = prepare_data(df_asteroids)

    # Initialize the model
    model = NeuralNetwork(input_size)
    train_model(model, x_train_data, y_train_data)

    # Evaluate the model on the test set
    with torch.no_grad():
        model.eval()
        test_outputs = model(x_test_data)
        test_outputs = (test_outputs > 0.5).float()
        accuracy = torch.eq(test_outputs, y_test_data).float().mean()
        print(f'Accuracy on test set: {accuracy.item():.4f}')
