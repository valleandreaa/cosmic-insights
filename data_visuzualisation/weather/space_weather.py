from data_warehouse.import_weather_data import get_data
import forecasting
import plots
import pandas as pd
import matplotlib.pyplot as plt
space_weather = get_data()

columns = ['time_tag', 'bx_gsm', 'by_gsm', 'bz_gsm', 'lon_gsm', 'lat_gsm', 'bt', 'density', 'speed', 'temperature',  'He', 'Hp', 'Hn', 'total']

df_solar_wind_mag = pd.DataFrame(space_weather, columns=columns)

# Remove 'time_tag' from the columns to forecast, as it's likely a timestamp
forecast_columns = [col for col in df_solar_wind_mag.columns if col != 'time_tag']

# Dictionary to store forecasts for each variable
forecasts = {}

# Loop through each column and apply forecasting
for column in forecast_columns:
    forecasts[column] = forecasting.forecast_variable(df_solar_wind_mag, column)


plots.plot_solar_wind_magnetic_field(space_weather)
plots.plot_solar_wind_plasma(space_weather)
plots.plot_magnetometer_data(space_weather)
plots.plot_protons_data(space_weather)
def plot_data_with_forecasts(df, forecasts, y_columns, y_labels, title, ylabel, xlabel='Time'):
    if not df.empty:
        plt.figure(figsize=(10, 6))
        time_interval = df['time_tag'].iloc[-1] - df['time_tag'].iloc[-2]
        forecast_start_time = df['time_tag'].iloc[-1] + time_interval

        for y_column, y_label in zip(y_columns, y_labels):
            plt.plot(df['time_tag'], df[y_column], label=y_label)

            # Plot forecasted data if available
            if y_column in forecasts:
                forecast_df = forecasts[y_column]
                # Create a range of time indices for forecasted data
                forecast_time_indices = pd.date_range(start=forecast_start_time, periods=len(forecast_df), freq=time_interval)
                plt.plot(forecast_time_indices, forecast_df.iloc[:, 0], label=f'{y_label} - Forecast', linestyle='--')

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.legend()
        plt.show()
    else:
        print("No data available for plotting")

plot_data_with_forecasts(space_weather, forecasts, ['bx_gsm', 'by_gsm', 'bz_gsm'], ['Bx GSM', 'By GSM', 'Bz GSM'], 'Solar Wind Magnetic Field Components', 'Magnetic Field (nT)')


