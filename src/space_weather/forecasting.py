import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split


class MultiInputForecast:
    def __init__(self, data):
        self.data = data
        self.features = list(data.columns)
        self.model = self._build_model()

    # Rest of the class methods...

    def _build_model(self):
        class LSTM(nn.Module):
            def __init__(self, input_size, hidden_layer_size, output_size):
                super().__init__()
                self.hidden_layer_size = hidden_layer_size
                self.lstm = nn.LSTM(input_size, hidden_layer_size)
                self.linear = nn.Linear(hidden_layer_size, output_size)
                self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size),
                                    torch.zeros(1, 1, self.hidden_layer_size))

            def forward(self, input_seq):
                lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq), 1, -1), self.hidden_cell)
                predictions = self.linear(lstm_out.view(len(input_seq), -1))
                return predictions[-1]

        # Determine input size based on the number of features
        input_size = len(self.features)
        model = LSTM(input_size, hidden_layer_size=100, output_size=1)
        return model

    # Rest of the methods...

    def train_model(self, epochs=150):
        loss_function = nn.MSELoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)

        for i in range(epochs):
            for seq, labels in zip(self.train_data, self.test_data):
                optimizer.zero_grad()
                self.model.hidden_cell = (
                    torch.zeros(1, 1, self.model.hidden_layer_size),
                    torch.zeros(1, 1, self.model.hidden_layer_size)
                )

                y_pred = self.model(seq)
                single_loss = loss_function(y_pred, labels)
                single_loss.backward()
                optimizer.step()

            if i % 25 == 1:
                print(f'Epoch: {i + 1:3} Loss: {single_loss.item():10.8f}')


def forecast_combined_features(self, future=50):
        self.model.eval()
        test_inputs = self.test_data[-1].tolist()

        for i in range(future):
            seq = torch.FloatTensor(test_inputs[-self.sequence_length:])
            with torch.no_grad():
                self.model.hidden = (
                    torch.zeros(1, 1, self.model.hidden_layer_size),
                    torch.zeros(1, 1, self.model.hidden_layer_size)
                )
                test_inputs.append(self.model(seq).item())

        actual_predictions = self.scaler.inverse_transform(np.array(test_inputs[self.sequence_length:]).reshape(-1, 1))
        return actual_predictions
