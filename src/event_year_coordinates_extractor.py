import pandas as pd
import os
import duckdb
import logging

class EventYearCoordinatesExtractor:
    def __init__(self,emr_path: str, event_years_coordinates_filepath: str):
        """
        Initializes the EventYearCoordinatesExtractor with the given EMR path.      
    Args:
            emr_path (str): The path to the raw EMR data.                
            event_years_coordinates_filepath (str): The path to save the extracted event years coordinates.
        """ 
        # Event will be extracted from encounters date
        self.encounters_path = emr_path + "/encounters.csv"        
        self.organizations_path = emr_path + '/organizations.csv'
        self.conditions_path = emr_path + '/conditions.csv'
        self.event_years_coordinates_filepath = event_years_coordinates_filepath
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def extract(self) -> pd.DataFrame:
               
        logging.info(f"Extracting encounters from {self.encounters_path}")
        encounters = pd.read_csv(self.encounters_path, delimiter=",")

        logging.info(f"Extracting organizations from {self.organizations_path}")
        organizations = pd.read_csv(self.organizations_path, delimiter=",")

        logging.info(f"Extracting conditions from {self.conditions_path}")
        conditions = pd.read_csv(self.conditions_path, delimiter=",")

        con = duckdb.connect()
        con.register('encounters', encounters)
        con.register('conditions', conditions)
        con.register('organizations', organizations)

        # Save the years coordinates to a CSV file
        # For precision, we will round the coordinates to 1 decimal place (11.1km x 11.1km at the equator)
        coordinates_decimal_places = 1                
        # Only consider 1980 onwards
        starting_year = 1980
        query = f"""
            with events as (
                select 
                    extract('year' FROM  CAST(e.stop AS TIMESTAMP))  as year,
                    c.code,
                    c.description,
                    round(o.LAT,{coordinates_decimal_places}) as LAT,
                    round(o.LON,{coordinates_decimal_places}) as LON
                from encounters e
                inner join organizations o on e.ORGANIZATION = o.id
                inner join conditions c on e.id = c.ENCOUNTER
            )
            select distinct
                year,
                LAT as latitude, 
                LON as longitude
            from events
            where year >= {starting_year}
            order by 1,2,3
        """        
        years_coordinates = con.execute(query).fetchdf()    
        years_coordinates.to_csv(self.event_years_coordinates_filepath, index=False)
    
def main():   
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    emr_path = f"gs://{os.getenv('RAW_BUCKET')}/{os.getenv('RAW_PATH')}"    
    event_years_coordinates_filepath = f"gs://{os.getenv('WEATHER_BUCKET')}/{os.getenv('EVENT_YEARS_COORDINATES_FILEPATH')}"
    extractor = EventYearCoordinatesExtractor(emr_path, event_years_coordinates_filepath)        
    extractor.extract()
    
if __name__ == "__main__":
    main()