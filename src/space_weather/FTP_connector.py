import pandas as pd
import requests
import time


class SpaceWeatherData:
    def __init__(self):
        pass

    def fetch_data(self, specific_url, sleep_duration=5):
        url = f"https://services.swpc.noaa.gov/{specific_url}"
        try:
            with requests.Session() as session:
                # Make the request
                response = session.get(url)
                response.raise_for_status()

                # Return the JSON data if successful
                return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"For {url}: {e.response.status_code}")
            print(f"Error response content: {e.response.text}")
            time.sleep(sleep_duration)
            print("Retrying...")
            return self.fetch_data(specific_url, sleep_duration * 2)

    @staticmethod
    def convert_to_dataframe(data):
        """Convert JSON data to a Pandas DataFrame with appropriate types."""
        if not data:
            return pd.DataFrame()

        pd.set_option("display.precision", 2)
        df = pd.DataFrame(data[1:], columns=data[0])
        df['time_tag'] = pd.to_datetime(df['time_tag'])

        # Exclude 'time_tag' and 'energy'
        non_time_tag_columns = df.columns[(df.columns != 'time_tag') & (df.columns != 'energy')]

        df[non_time_tag_columns] = df[non_time_tag_columns].apply(pd.to_numeric, errors='coerce')
        df[non_time_tag_columns] = df[non_time_tag_columns].round(2)

        if 'energy' in df.columns:
            df['energy'] = df['energy'].str.extract(r'(\d+)').astype(float)

        return df

    def get_data(self):
        # Same structure like in Data Lake
        all_content = {
            "mag": self.fetch_data("products/solar-wind/mag-1-day.json"),
            "plasma": self.fetch_data("products/solar-wind/plasma-1-day.json"),
            "magnetometers": self.fetch_data("json/goes/primary/magnetometers-1-day.json"),
            "protons": self.fetch_data("json/goes/primary/integral-protons-1-day.json")
        }

        mag = self.convert_to_dataframe(all_content["mag"])
        plasma = self.convert_to_dataframe(all_content["plasma"])
        magnetometers = self.convert_to_dataframe(all_content["magnetometers"])
        protons = self.convert_to_dataframe(all_content["protons"])

        return mag, plasma, magnetometers, protons


if __name__ == "__main__":
    space_weather = SpaceWeatherData()
    df_mag, df_plasma, df_magnetometers, df_protons = space_weather.get_data()

    if not df_mag.empty:
        print(df_mag)
    else:
        print("Failed to retrieve or process primary data.")
