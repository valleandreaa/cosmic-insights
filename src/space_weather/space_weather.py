import plots
import FTP_connector
import forecasting

space_weather = FTP_connector.SpaceWeatherData()
df_solar_wind_mag, df_solar_wind_plasma, df_magnetometers, df_protons = space_weather.get_data()

plots.plot_solar_wind_magnetic_field(df_solar_wind_mag)
plots.plot_solar_wind_plasma(df_solar_wind_plasma)
plots.plot_magnetometer_data(df_magnetometers)
plots.plot_protons_data(df_protons)

future_predictions = forecasting.forecast_solar_wind(df_solar_wind_mag)

print("Predicted Values:", future_predictions)

# model_solar_wind_mag = forecasting.MultiInputForecast(df_solar_wind_mag)
# predictions_solar_wind_mag = model_solar_wind_mag.forecast_combined_features()
#
#
# model_solar_wind_plasma = forecasting.MultiInputForecast(df_solar_wind_plasma)
# predictions_solar_wind_plasma = model_solar_wind_plasma.forecast_combined_features()
#
# model_magnetometers = forecasting.MultiInputForecast(df_magnetometers)
# predictions_magnetometers = model_magnetometers.forecast_combined_features()
#
# model_protons = forecasting.MultiInputForecast(df_protons)
# predictions_protons = model_protons.forecast_combined_features()
