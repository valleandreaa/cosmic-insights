import matplotlib.pyplot as plt
import pandas as pd


def plot_data(df, y_columns, y_labels, title, ylabel, xlabel='Time'):
    if not df.empty:
        df['time_tag'] = pd.to_datetime(df['time_tag'])

        plt.figure(figsize=(10, 6))
        for y_column, y_label in zip(y_columns, y_labels):
            plt.plot(df['time_tag'], df[y_column], label=y_label)

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")


def plot_solar_wind_magnetic_field(df_solar_wind_mag):
    y_columns = ['bx_gsm', 'by_gsm', 'bz_gsm']
    y_labels = ['Bx GSM', 'By GSM', 'Bz GSM']
    plot_data(df_solar_wind_mag, y_columns, y_labels, 'Solar Wind Magnetic Field Components', 'Magnetic Field (nT)')


def plot_solar_wind_plasma(df_solar_wind_plasma):
    y_columns = ['density', 'speed']
    y_labels = ['Density', 'Speed']
    plot_data(df_solar_wind_plasma, y_columns, y_labels, 'Solar Wind Plasma Data', 'Values')


def plot_magnetometer_data(df_magnetometers):
    y_columns = ['He', 'Hp', 'Hn', 'total']
    y_labels = ['He', 'Hp', 'Hn', 'Total']
    plot_data(df_magnetometers, y_columns, y_labels, 'Magnetometer Data', 'Magnetic Field (nT)')


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
