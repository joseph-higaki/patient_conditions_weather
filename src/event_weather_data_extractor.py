import os
import pandas as pd
import logging  
from weather_data_extractor import WeatherDataExtractor

class EventWeatherDataExtractor:
    def __init__(self, event_years_coordinates_filepath: str, destination_bucket_name: str, destination_path: str, force_extract:bool=False):
        self.event_years_coordinates_filepath = event_years_coordinates_filepath
        self.destination_bucket_name = destination_bucket_name
        self.destination_path = destination_path
        self.force_extract = force_extract
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def extract(self):  
        """	    
        Extracts weather data for the event years coordinates
        """
        logging.info(f"Extracting event years coordinates from {self.event_years_coordinates_filepath}")
        event_years_coordinates = pd.read_csv(self.event_years_coordinates_filepath, delimiter=",")
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