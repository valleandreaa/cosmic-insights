import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


class LSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTM, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(device)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out


def create_sequences(data, seq_length):
    sequences = []
    for i in range(len(data) - seq_length):
        sequence = data[i: i + seq_length]
        sequences.append(sequence)
    return np.array(sequences)


def forecast_solar_wind(df_solar_wind_mag, sequence_length=10, hidden_size=64, num_layers=2, num_epochs=3,
                        num_steps_ahead=5):
    df_solar_wind_mag = df_solar_wind_mag[['bx_gsm']]
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df_solar_wind_mag)

    sequences = create_sequences(scaled_data, sequence_length)
    x = torch.from_numpy(sequences[:, :-1]).float()
    y = torch.from_numpy(sequences[:, -1][:, -1]).float()

    model = LSTM(input_size=len(df_solar_wind_mag.columns), hidden_size=hidden_size, num_layers=num_layers,
                 output_size=1).to(device)

    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    for epoch in range(num_epochs):
        model.train()
        outputs = model(x.to(device))
        optimizer.zero_grad()
        loss = criterion(outputs.view(-1), y.to(device))
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 10 == 0:
            print(f'Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}')

    model.eval()
    with torch.no_grad():
        future_sequence = x[-1].unsqueeze(0)
        future_sequence = future_sequence.to(device)
        future_predictions = []

        for _ in range(num_steps_ahead):
            future_prediction = model(future_sequence)
            future_predictions.append(future_prediction.cpu().numpy()[0, 0])
            new_input = torch.cat((future_sequence[:, 1:, :], future_prediction.unsqueeze(1)), dim=1)
            future_sequence = new_input

        future_predictions = scaler.inverse_transform([future_predictions])
        predictions_df = pd.DataFrame(future_predictions.T, columns=['Predicted_Values'])

    return predictions_df
