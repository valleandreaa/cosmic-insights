from src.space_weather import plots
from src.space_weather import FTP_connector

space_weather = FTP_connector.SpaceWeatherData()

solar_wind_mag = space_weather.get_data_space_wind_mag()
df_solar_wind_mag = space_weather.convert_to_dataframe(solar_wind_mag)

solar_wind_plasma = space_weather.get_data_space_wind_plasma()
df_solar_wind_plasma = space_weather.convert_to_dataframe(solar_wind_plasma)

magnetometers = space_weather.get_data_magnetometers()
df_magnetometers = space_weather.convert_to_dataframe(magnetometers)

protons = space_weather.get_data_protons()
df_protons = space_weather.convert_to_dataframe(protons)



plots.plot_solar_wind_magnetic_field(df_solar_wind_mag)
plots.plot_solar_wind_plasma(df_solar_wind_plasma)
plots.plot_magnetometer_data(df_magnetometers)
plots.plot_protons_data(df_protons)
