import requests
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import argparse
from tenacity import retry, stop_after_attempt, wait_random_exponential
import time
import random

import pyarrow.dataset as ds


DAILY = 'daily'
HOURLY = 'hourly'
OPEN_METEO_API_URL = "https://archive-api.open-meteo.com/v1/archive"

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=1, max=10))
def get_weather_api_data(url, params):
    wait_time = random.uniform(1, 3)
    time.sleep(wait_time)
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response

# src/extract/weather_data_extractor.py
class WeatherDataExtractor:
    def __init__(self,  year: int, latitude: float, longitude: float, destination_bucket_name: str, destination_path: str, force_extract:bool=False):
        """
        Initialize Weather Data Extraction process
        
        Args:
            year (int): Year of the weather data
            latitude (float): Latitude. Rounded to 3 decimal places
            longitude (float): Longitude. Rounded to 3 decimal places
            destination_bucket_name (str): Bucket for storing raw data
            destination_path (str): path within the Bucket for storing raw data
            force_extract (bool): Force extraction of the data. When True, the data is extracted even if it already exists at the destination bucket / path
        """
        if year is not None:
            self.year = year
        if latitude is not None:
            self.latitude = round(float(latitude), 3) 
        if longitude is not None:
            self.longitude = round(float(longitude), 3) 
        self.destination_bucket_name = destination_bucket_name
        self.destination_path = destination_path
        self.force_extract = force_extract
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def validate_frequency(frequency: str) -> None:
        """ Validate the frequency parameter 
        Args:   
            frequency (str): Frequency of the weather data (daily or hourly)
        """ 
        if frequency not in [DAILY, HOURLY]:
            raise ValueError(f"Invalid frequency: {frequency}")

    def get_weather_stored_data(self, 
                                frequency: str = DAILY, 
                                year: int = None, 
                                latitude: float = None, 
                                longitde: float = None) -> pd.DataFrame:
        """
        Get weather data from the stored parquet files
        Args:
            frequency (str): Frequency of the weather data (daily or hourly)
            year (int): Year of the weather data
            latitude (float): Latitude. Rounded to 3 decimal places
            longitude (float): Longitude. Rounded to 3 decimal places
        """
        self.__class__.validate_frequency(frequency)
        data = None
        #try:
            #path = self.get_full_path() + f"/frequency={frequency}/year={self.year}/latitude={self.latitude}/longitude={self.longitude}"
            #logging.info(f"Reading stored data from {path}")
            #data = pd.read_parquet(path, engine="pyarrow")
        #except FileNotFoundError:
        #    print(f"Data not found for year={self.year}, latitude={self.latitude}, longitude={self.longitude}")    

        base_path = self.get_full_path() #+ f"/frequency={frequency}"
        logging.info(f"Reading stored data from {base_path}")
        
        dataset = ds.dataset(base_path, format="parquet", partitioning="hive")

        # Build a filter dynamically for your query
        filter_expression = ds.field("frequency") == frequency
        
        if year is not None:
            filter_expression &= ds.field("year") == year

        # Format latitude and longitude to match the string type in Hive partitions
        if latitude is not None:
            filter_expression &= ds.field("latitude") == f"{latitude:.1f}" 

        if longitde is not None:
            filter_expression &= ds.field("longitude") == f"{longitde:.1f}"

        logging.info(f"Querying dataset with filter: {filter_expression}")
        data = dataset.to_table(filter=filter_expression).to_pandas()
        if data.empty:
            logging.info(f"Data not found for year={self.year}, latitude={self.latitude}, longitude={self.longitude}")
        return data

    def exists_weather_cached_data(self, frequency: str):
        data = self.get_weather_stored_data(frequency, self.year, self.latitude, self.longitude)
        # Simple check if the data is not empty
        # This can be improved by:
        # - Checking the number of records  (can use parquet file metadata)
        # - - Leap Year:
        # - - - Daily: 366 records
        # - - - Hourly: 8784 records
        # - - Non-Leap Year:
        # - - - Daily: 365 records  
        # - - - Hourly: 8760 records
        if frequency == DAILY:
            return data is not None and not data.empty
        elif frequency == HOURLY:
            return data is not None and not data.empty

    def extract(self):
        """ Fetch weather data from the Open-Meteo API for a given year and location      
            Store the data in a Google Cloud Storage bucket 
        """                  
        start_date = datetime(self.year, 1, 1)
        if self.year > datetime.now().year:
            logging.error(f"Year {self.year} is in the future. Weather data is not available for future dates.")
            return

        end_date = min(datetime(self.year, 12, 31), datetime.now())
        logging.info(f"Extracting weather data for {self.year} at {self.latitude}, {self.longitude} from {start_date} to {end_date}")

        if not self.force_extract:
            # Check if the file already exists
            logging.info(f"Checking if year:{self.year} latitude: {self.latitude} longitude: {self.longitude} already extracted in {self.get_full_path()}")       
            exists_daily = self.exists_weather_cached_data(DAILY)
            #exists_hourly = self.exists_weather_cached_data(HOURLY)
            if exists_daily: # and exists_hourly:
                logging.info(f"Data already extracted for year:{self.year} latitude: {self.latitude} longitude: {self.longitude}")
                return

        # Define the API URL
        url = OPEN_METEO_API_URL
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),  
            #"hourly": "temperature_2m,cloud_cover",
            "daily": "weather_code,temperature_2m_mean,apparent_temperature_mean,daylight_duration,sunshine_duration",
        }
        # Make the GET request
        logging.info(f"Fetching weather data for {self.year} at {self.latitude}, {self.longitude}")        
        # Uses tenacity to retry the request in case of failure and wait in between retries
        response = get_weather_api_data(url, params)
        # Parse the JSON response
        data = response.json()

        # Extract daily data Create a DataFrame
        daily_df = pd.DataFrame(data[DAILY])
        # Convert the "time" column to datetime with UTC timezone
        daily_df["time"] = pd.to_datetime(daily_df["time"], format="%Y-%m-%d").dt.tz_localize("UTC")
        # Convert numerical columns to float
        daily_float_columns = ["temperature_2m_mean", "apparent_temperature_mean", "daylight_duration", "sunshine_duration"]
        daily_df[daily_float_columns] = daily_df[daily_float_columns].astype(float)
        daily_df["weather_code"] = daily_df["weather_code"].astype(int)
        # Rename columns for clarity
        daily_df.rename(columns={"time": "date"}, inplace=True)
        daily_df["year"] = self.year
        formatted_latitude = f"{self.latitude:.1f}"        
        formatted_longitude = f"{self.longitude:.1f}"
        daily_df["latitude"] = formatted_latitude
        daily_df["longitude"] = formatted_longitude
        daily_df["frequency"] = DAILY
        
        logging.info(f"Extracted {len(daily_df)} daily records")
        self.store_to_partition(daily_df, ['frequency', 'year', 'latitude', 'longitude'])

    def get_full_path(self) -> str:
        return f"gs://{self.destination_bucket_name }/{self.destination_path}"

    def store_to_partition(self, df, partition_columns):
            table = pa.Table.from_pandas(df)
            destination_full_path = self.get_full_path()
            logging.info(f"Storing to partition in path: {destination_full_path}")
            pq.write_to_dataset(
                table,
                root_path=destination_full_path,
                partition_cols=partition_columns
            )      

if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    parser = argparse.ArgumentParser(description="Extract weather data")
    parser.add_argument("--year", type=int, help="Year of the weather data")
    parser.add_argument("--latitude", type=lambda x: round(float(x), 3), help="Latitude (rounded to 3 decimal places)")     
    parser.add_argument("--longitude", type=lambda x: round(float(x), 3), help="Longitude (rounded to 3 decimal places)")
    parser.add_argument("--destination_bucket_name", type=str, help="Bucket for storing raw data")
    parser.add_argument("--destination_path", type=str, help="path within the Bucket for storing raw data")
        
    args = parser.parse_args()
    extractor = WeatherDataExtractor(args.year, args.latitude, args.longitude, args.destination_bucket_name, args.destination_path)
    extractor.extract()

    '''
    python src/weather_data_extractor.py --year 2021 --latitude 41.3 --longitude 2.1 --destination_bucket_name patient-weather-data --destination_path weather-data-raw/weather-data

    python src/weather_data_extractor.py --year 1980 --latitude 41.6 --longitude -71.0 --destination_bucket_name patient-weather-data --destination_path weather-data-raw/weather-data
    python -m debugpy src/weather_data_extractor.py --year 2021 --latitude 41.3 --longitude 2.1 --destination_bucket_name patient-weather-data --destination_path weather-data-raw/weather-data
    '''