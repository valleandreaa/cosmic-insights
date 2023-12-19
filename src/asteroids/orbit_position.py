from astropy.time import Time
from poliastro.bodies import Earth
from poliastro.twobody import Orbit
import astropy.units as u
import requests


def calculate_current_orbit(neo_data):
    # Convert angle values to radians
    inclination = float(neo_data['orbital_data']['inclination']) * u.deg
    ascending_node = float(neo_data['orbital_data']['ascending_node_longitude']) * u.deg
    perihelion_arg = float(neo_data['orbital_data']['perihelion_argument']) * u.deg
    true_anomaly = 0.0 * u.deg

    orb = Orbit.from_classical(
        Earth,
        a=float(neo_data['orbital_data']['semi_major_axis']) * u.AU,  # the size of the orbit
        ecc=float(neo_data['orbital_data']['eccentricity']) * u.one,  # the shape of the orbit
        inc=inclination,  # the tilt of the orbit
        raan=ascending_node,  # rotation of the orbit
        argp=perihelion_arg,  # the orientation of the orbit
        nu=true_anomaly,  # angular distance of the NEO from the periapsis at the current time
        epoch=Time(neo_data['orbital_data']['epoch_osculation'], format='jd')  # reference time for the orbital elements
    )

    current_time = Time.now()
    closest_approach_time = Time(neo_data['orbital_data']['orbit_determination_date'], format='iso')
    time_since_closest_approach = current_time - closest_approach_time
    orb_at_current_time = orb.propagate(time_since_closest_approach)

    return orb_at_current_time


if __name__ == "__main__":
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse?page=1&size=1&api_key=U1rN3FyZdsp9dpqaKIFRRAl92EtaBV9AUGdb8lei"
    r = requests.get(url)
    data = r.json()

    # Calculate current orbit
    current_orbit = calculate_current_orbit(data['near_earth_objects'][0])
    x, y, z = current_orbit.r

    print(f"Position of the asteroid at the current time: ({x.to(u.km):.2f}, {y.to(u.km):.2f}, {z.to(u.km):.2f}) km")