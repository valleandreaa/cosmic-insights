import requests
from datetime import datetime
import pandas as pd
import toml

class SpaceWeatherData:
    timeout = 10

    def __init__(self):
        pass

    def get_data_space_wind_mag(self):
        """Get primary data from the specified URL."""
        data = toml.load('config.toml')
        paths = data.get('PATHS', {})
        url = paths.get('solar_wind_mag', '')
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return {}

    def get_data_space_wind_plasma(self):
        """Get primary data from the specified URL."""
        data = toml.load('config.toml')
        paths = data.get('PATHS', {})
        url = paths.get('solar_wind_plasma', '')
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return {}

    def get_data_magnetometers(self):
        """Get primary data from the specified URL."""
        data = toml.load('config.toml')
        paths = data.get('PATHS', {})
        url = paths.get('magnetometers', '')
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return {}

    def get_data_protons(self):
        """Get primary data from the specified URL."""
        data = toml.load('config.toml')
        paths = data.get('PATHS', {})
        url = paths.get('protons', '')
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return {}
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return {}

    def extract_energy_number(self, energy):
        """Extract numeric value from 'energy' field."""
        energy = energy.replace('>=', '').split(' ')[0]
        try:
            return float(energy)
        except ValueError:
            return None

    def convert_to_dataframe(self, data):
        """Convert JSON data to a Pandas DataFrame with appropriate types."""
        if not data:
            return pd.DataFrame()

        pd.set_option("display.precision", 2)
        df = pd.DataFrame(data[1:], columns=data[0])
        df['time_tag'] = pd.to_datetime(df['time_tag'])

        non_time_tag_columns = df.columns[(df.columns != 'time_tag') & (df.columns != 'energy')]  # Exclude 'time_tag' and 'energy'

        df[non_time_tag_columns] = df[non_time_tag_columns].apply(pd.to_numeric, errors='coerce')
        df[non_time_tag_columns] = df[non_time_tag_columns].round(2)

        if 'energy' in df.columns:
            def extract_energy_number(energy):
                parts = energy.split(' ')
                for part in parts:
                    if part.replace('.', '', 1).isdigit():  # Check if part is a number
                        return float(part)
                return None

            df['energy'] = df['energy'].apply(self.extract_energy_number)
        return df

if __name__ == "__main__":

    space_weather = SpaceWeatherData()

    solar_wind_mag    = space_weather.get_data_space_wind_mag()
    df_solar_wind_mag = space_weather.convert_to_dataframe(solar_wind_mag)

    solar_wind_plasma    = space_weather.get_data_space_wind_plasma()
    df_solar_wind_plasma = space_weather.convert_to_dataframe(solar_wind_plasma)

    magnetometers    = space_weather.get_data_magnetometers()
    df_magnetometers = space_weather.convert_to_dataframe(magnetometers)

    protons    = space_weather.get_data_protons()
    df_protons = space_weather.convert_to_dataframe(protons)

    if not df_solar_wind_mag.empty:
        print(df_solar_wind_mag)
    else:
        print("Failed to retrieve or process primary data.")