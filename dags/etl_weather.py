from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime

# Latitude and Longitude for London
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

# define connection IDs
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"

default_args = {
    "owner": "airflow",
    'start_date': datetime(2025, 7, 20),  # Set a start date for the DAG
}

# Define the DAG
with DAG(
    dag_id="etl_weather_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False, # Do not backfill(run previous dates)
) as dag:
    @task()
    def extract_weather_data():
        """Extracts weather data from the Open Meteo API for London."""

        # use http hook to get connection details from aiflow connections
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)

        #build the API endpoint 
        endpoint = f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        
        # make the API request 
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")


    @task()
    def transform_weather_data(weather_data):
        """Transforms the weather data."""
        current_weather = weather_data["current_weather"]
        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current_weather["temperature"],
            "windspeed": current_weather["windspeed"],
            "winddirection": current_weather["winddirection"],
            "weathercode": current_weather["weathercode"],
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        """Loads the transformed weather data into a PostgreSQL database."""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
            """, ( 
            transformed_data["latitude"],
            transformed_data["longitude"],
            transformed_data["temperature"],
            transformed_data["windspeed"],
            transformed_data["winddirection"],
            transformed_data["weathercode"],
        ))

        conn.commit()
        cursor.close()
        conn.close()

    # Define the ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
