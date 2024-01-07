# Cosmic Insights Navigating Threats and Wonders

## Overview

**Cosmic Insights** is an advanced data-driven project that provides comprehensive insights into near-earth objects (NEOs) and space weather phenomena. Using data from NASA's NEO APIs and space weather sources, the project offers detailed analysis and predictions about asteroid characteristics and their potential threats, along with critical information on space weather and its impacts.

## Features

- **ETL Pipeline**: Automated pipeline using Apache Airflow for extracting NEO and space weather data, transforming it, and loading into a RDS database.
- **Threat Prediction Model**: Machine Learning model to assess and predict the threat levels of asteroids.
- **Space Weather Analysis**: Tools and methodologies to understand and predict space weather events.
- **Data Analysis and Reporting**: Comprehensive tools for analyzing the data and generating insightful reports.

## Installation



### Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/valleandreaa/cosmic-insights.git
   cd cosmic-insights
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```



## Usage

### Airflow DAGs

1. Start Airflow:
   ```bash
   airflow webserver -p 8080
   airflow scheduler
   ```
2. Access the Airflow UI at `http://localhost:8080`.
3. Trigger the DAGs to process NEO and space weather data.


## License

This project is licensed under the [MIT License](LICENSE).

