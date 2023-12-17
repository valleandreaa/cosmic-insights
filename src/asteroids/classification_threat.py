import requests
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Function to fetch NEO data
def fetch_neo_data():
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?page=1&size=10&api_key=U1rN3FyZdsp9dpqaKIFRRAl92EtaBV9AUGdb8lei"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['near_earth_objects']
    else:
        print("Failed to retrieve data from the API")
        return None

# Fetch NEO data
neo_data = fetch_neo_data()

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
    distance_from_earth = float(closest_approach_data[0]['miss_distance'].get('kilometers', 0)) if closest_approach_data else 0

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

# Split data into features and target variable
X = extracted_data.drop('threat_level', axis=1)
y = extracted_data['threat_level']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Normalize features using StandardScaler
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Convert data to PyTorch tensors
X_train_tensor = torch.tensor(X_train, dtype=torch.float32)
X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
y_train_tensor = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)
y_test_tensor = torch.tensor(y_test.values, dtype=torch.float32).view(-1, 1)

# Define a simple neural network model
class NeuralNetwork(nn.Module):
    def __init__(self, input_size):
        super(NeuralNetwork, self).__init__()
        self.fc1 = nn.Linear(input_size, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.sigmoid(self.fc1(x))
        x = self.sigmoid(self.fc2(x))
        x = self.sigmoid(self.fc3(x))
        return x

# Initialize the model
input_size = X_train.shape[1]
model = NeuralNetwork(input_size)

# Define loss function and optimizer
criterion = nn.BCELoss()  # Binary Cross Entropy Loss
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Training the model
num_epochs = 1000
for epoch in range(num_epochs):
    optimizer.zero_grad()
    outputs = model(X_train_tensor)
    loss = criterion(outputs, y_train_tensor)
    loss.backward()
    optimizer.step()

    if (epoch + 1) % 100 == 0:
        print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.4f}')

torch.save(model.state_dict(), 'threat_model.pt')

# Evaluate the model on the test set
with torch.no_grad():
    model.eval()
    test_outputs = model(X_test_tensor)
    test_outputs = (test_outputs > 0.5).float()
    accuracy = (test_outputs == y_test_tensor).float().mean()
    print(f'Accuracy on test set: {accuracy.item():.4f}')
