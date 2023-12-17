import matplotlib.pyplot as plt
import pandas as pd

def plot_solar_wind_magnetic_field(df_solar_wind_mag):
    if not df_solar_wind_mag.empty:
        df_solar_wind_mag['time_tag'] = pd.to_datetime(df_solar_wind_mag['time_tag'])

        plt.figure(figsize=(10, 6))
        plt.plot(df_solar_wind_mag['time_tag'], df_solar_wind_mag['bx_gsm'], label='Bx GSM')
        plt.plot(df_solar_wind_mag['time_tag'], df_solar_wind_mag['by_gsm'], label='By GSM')
        plt.plot(df_solar_wind_mag['time_tag'], df_solar_wind_mag['bz_gsm'], label='Bz GSM')

        plt.xlabel('Time')
        plt.ylabel('Magnetic Field (nT)')
        plt.title('Solar Wind Magnetic Field Components')
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")


def plot_solar_wind_plasma(df_solar_wind_plasma):
    if not df_solar_wind_plasma.empty:
        df_solar_wind_plasma['time_tag'] = pd.to_datetime(df_solar_wind_plasma['time_tag'])

        plt.figure(figsize=(10, 6))
        plt.plot(df_solar_wind_plasma['time_tag'], df_solar_wind_plasma['density'], label='Density')
        plt.plot(df_solar_wind_plasma['time_tag'], df_solar_wind_plasma['speed'], label='Speed')

        plt.xlabel('Time')
        plt.ylabel('Values')
        plt.title('Solar Wind Plasma Data')
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")


def plot_magnetometer_data(df_magnetometers):
    if not df_magnetometers.empty:
        df_magnetometers['time_tag'] = pd.to_datetime(df_magnetometers['time_tag'])

        plt.figure(figsize=(10, 6))
        plt.plot(df_magnetometers['time_tag'], df_magnetometers['He'], label='He')
        plt.plot(df_magnetometers['time_tag'], df_magnetometers['Hp'], label='Hp')
        plt.plot(df_magnetometers['time_tag'], df_magnetometers['Hn'], label='Hn')
        plt.plot(df_magnetometers['time_tag'], df_magnetometers['total'], label='Total')

        plt.xlabel('Time')
        plt.ylabel('Magnetic Field (nT)')
        plt.title('Magnetometer Data')
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")


def plot_protons_data(df_protons):
    if not df_protons.empty:
        df_protons['time_tag'] = pd.to_datetime(df_protons['time_tag'])

        # Filter for specific energy levels (10, 50, 100 MeV)
        selected_energy_levels = [10.0, 50.0, 100.0]
        df_selected_protons = df_protons[df_protons['energy'].isin(selected_energy_levels)]

        # Group data by 'energy' and plot flux over time for each energy level
        fig, ax = plt.subplots(figsize=(10, 6))
        for energy_level, group in df_selected_protons.groupby('energy'):
            ax.plot(group['time_tag'], group['flux'], label=energy_level)

        plt.xlabel('Time')
        plt.ylabel('Proton Energy (MeV)')
        plt.title('Protons Data')
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")


