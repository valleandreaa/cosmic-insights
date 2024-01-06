import matplotlib.pyplot as plt


def plot_data(df, y_columns, y_labels, title, ylabel, xlabel='Time'):
    if not df.empty:
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


def plot_generic_data(data, y_columns, y_labels, title, ylabel):
    df_selected = data[['time_tag'] + y_columns].dropna(subset=y_columns).dropna()

    if not df_selected.empty:
        plot_data(df_selected, y_columns, y_labels, title, ylabel)
    else:
        print(f"No data available for plotting {title}")


def plot_solar_wind_magnetic_field(data):
    y_columns = ['bx_gsm', 'by_gsm', 'bz_gsm']
    y_labels = ['Bx GSM', 'By GSM', 'Bz GSM']
    plot_generic_data(data, y_columns, y_labels, 'Solar Wind Magnetic Field Components', 'Magnetic Field (nT)')


def plot_solar_wind_plasma(data):
    y_columns = ['density', 'speed']
    y_labels = ['Density', 'Speed']
    plot_generic_data(data, y_columns, y_labels, 'Solar Wind Plasma Data', 'Values')


def plot_magnetometer_data(data):
    y_columns = ['He', 'Hp', 'Hn', 'total']
    y_labels = ['He', 'Hp', 'Hn', 'Total']
    plot_generic_data(data, y_columns, y_labels, 'Magnetometer Data', 'Magnetic Field (nT)')


def plot_protons_data(data):
    df_selected = data[['time_tag', 'energy', 'flux']].dropna(subset=['energy', 'flux'])

    if not df_selected.empty:
        # Count occurrences of each unique value in 'energy'
        value_counts = df_selected['energy'].value_counts()

        # Select unique values that appear more than twice
        selected_energy_levels = value_counts[value_counts > 2].sort_index().index.tolist()
        df_selected_protons = df_selected[df_selected['energy'].isin(selected_energy_levels)]

        # Group data by 'energy' and plot flux over time for each energy level
        fig, ax = plt.subplots(figsize=(10, 6))
        for energy_level, group in df_selected_protons.groupby('energy'):
            ax.plot(group['time_tag'], group['flux'], label=f'{energy_level} MeV')

        plt.xlabel('Time')
        plt.ylabel('Flux')
        plt.title('Protons Data')
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")
