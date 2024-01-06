from data_warehouse.import_weather_data import get_data
import forecasting
import plots

space_weather = get_data()
plots.plot_solar_wind_magnetic_field(space_weather)
plots.plot_solar_wind_plasma(space_weather)
plots.plot_magnetometer_data(space_weather)
plots.plot_protons_data(space_weather)

future_predictions = forecasting.forecast_solar_wind(space_weather)

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
