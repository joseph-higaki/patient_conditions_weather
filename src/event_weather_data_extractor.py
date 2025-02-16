import os
import pandas as pd
import logging  
from weather_data_extractor import WeatherDataExtractor
import random

class EventWeatherDataExtractor:
    def __init__(self, event_years_coordinates_filepath: str, destination_bucket_name: str, destination_path: str, force_extract:bool=False):
        self.event_years_coordinates_filepath = event_years_coordinates_filepath
        self.destination_bucket_name = destination_bucket_name
        self.destination_path = destination_path
        self.force_extract = force_extract
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def filter_event_years_coordinates_without_weather(self, event_years_coordinates: pd.DataFrame):
        """
        Filters event years coordinates that do not have weather data
        """
        extractor = WeatherDataExtractor(None, None, None, self.destination_bucket_name, self.destination_path)
        daily_weather_data = extractor.get_weather_stored_data()

        # Select relevant columns for comparison
         # Let's first check the data types
        logging.info("Event years dtypes: %s", event_years_coordinates[['year', 'latitude', 'longitude']].dtypes)
        logging.info("Weather data dtypes: %s", daily_weather_data[['year', 'latitude', 'longitude']].dtypes)
        
        # Convert data types to be consistent
        events_subset = event_years_coordinates[['year', 'latitude', 'longitude']].astype({
            'year': 'int64',
            'latitude': 'float64',
            'longitude': 'float64'
        })
        
        weather_subset = daily_weather_data[['year', 'latitude', 'longitude']].astype({
            'year': 'int64',
            'latitude': 'float64',
            'longitude': 'float64'
        })
         # Create a unique key for comparison
        events_subset['key'] = events_subset.apply(lambda x: f"{x['year']}_{x['latitude']}_{x['longitude']}", axis=1)
        weather_subset['key'] = weather_subset.apply(lambda x: f"{x['year']}_{x['latitude']}_{x['longitude']}", axis=1)
        
        # Find missing combinations
        missing_keys = set(events_subset['key']) - set(weather_subset['key'])
        logging.info(f"Missing keys count: {len(missing_keys)}")
        
        return event_years_coordinates[events_subset['key'].isin(missing_keys)]
        # Return rows that have missing weather data
        #event_years_coordinates = event_years_coordinates[events_subset['key'].isin(missing_keys)]
        #event_years_coordinates_with_weather = pd.DataFrame()
        #for index, row in event_years_coordinates.iterrows():
        #    year = row['year'].astype(int)
        #    latitude = row['latitude']
        #    longitude = row['longitude']
        #    weather_data_path = f"{self.destination_path}/{year}/{latitude}_{longitude}.csv"
        #    if not os.path.exists(weather_data_path):
        #        event_years_coordinates_with_weather = event_years_coordinates_with_weather.append(row)
        #return event_years_coordinates_with_weather

    def extract(self):  
        """	    
        Extracts weather data for the event years coordinates
        """        
        #import random 
        #skip_rows= int(random.uniform(1, 4)*1000)        
        #range_to_include = range(int(skip_rows/2), skip_rows)
        #logging.info(f"Extracting event years coordinates from {self.event_years_coordinates_filepath} considering {range_to_include} rows")
        #event_years_coordinates = pd.read_csv(self.event_years_coordinates_filepath, delimiter=",",skiprows=lambda x: x in [0, range_to_include])
        logging.info(f"Extracting event years coordinates from {self.event_years_coordinates_filepath}")
        original_event_years_coordinates = pd.read_csv(self.event_years_coordinates_filepath, delimiter=",")
        event_years_coordinates = self.filter_event_years_coordinates_without_weather(original_event_years_coordinates)

        columns = random.sample(['latitude', 'longitude', 'year'], k=3)
        ascending = [random.choice([True, False]) for _ in columns]
        event_years_coordinates.sort_values(by=columns, ascending=ascending, inplace=True)

        for index, row in event_years_coordinates.iterrows():
            year = row['year'].astype(int)
            latitude = row['latitude']
            longitude = row['longitude']
            logging.info(f"Extracting weather data for year {year}, latitude {latitude}, longitude {longitude}")
            extractor = WeatherDataExtractor(year, latitude, longitude, self.destination_bucket_name, self.destination_path, self.force_extract)
            extractor.extract()

            previous_year = year - 1
            if event_years_coordinates[
                    (event_years_coordinates['year'] == previous_year) 
                    & (event_years_coordinates['latitude'] == latitude) 
                    & (event_years_coordinates['longitude'] == longitude)].empty:                
                logging.info(f"Extracting weather data for previous year {previous_year}, latitude {latitude}, longitude {longitude}")
                extractor = WeatherDataExtractor(previous_year, latitude, longitude, self.destination_bucket_name, self.destination_path, self.force_extract)
                extractor.extract()            
        logging.info("Extraction complete")

def main():   
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    event_years_coordinates_filepath = f"gs://{os.getenv('WEATHER_BUCKET')}/{os.getenv('EVENT_YEARS_COORDINATES_FILEPATH')}"
    destination_bucket_name = os.getenv("WEATHER_BUCKET")
    destination_path = os.getenv("WEATHER_DATA_DAILY_PATH")
    extractor = EventWeatherDataExtractor(event_years_coordinates_filepath, destination_bucket_name, destination_path)
    extractor.extract()
    
if __name__ == "__main__":
    main()