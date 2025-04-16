import pandas as pd
import sys
import os
from dotenv import load_dotenv # Import load_dotenv lo import .env file

from omop_mapper import DataMapper



load_dotenv()

def load_synthea_data_from_file(data_path: str):
    """
        Load synthea data from file

        Args:
            data_path (str): Path to Synthea data file.

        Returns:
            csv_file: A panda dataframe
    """
    try:
        csv_file = pd.read_csv(data_path)
    except FileNotFoundError as e:
        print("Error in load_synthea_data:  File not found", e)
    except pd.errors.EmptyDataError:
        print(f"Error in load_synthea_data:  Error: The file at the path {data_path} is empty")
    except pd.errors.ParserError:
        print(f"Error in load_synthea_data:  Error: File at the path {data_path} could not be parsed. Check for formatting issues.")
    except Exception as e:
        print(f"Error in load_synthea_data: An unexpected error occurred: {e}")
    return csv_file


def transform_synthea_data_to_omop(synthea_data: dict):
    """
        Transform Synthea data into OMOP CDM format.

        Args:
            synthea_data (dict): A dictionary of DataFrames from load_synthea_data_from_file().

        Returns:
            dict: A dictionary of DataFrames, where keys are OMOP table names
                  (e.g., 'person', 'visit_occurrence').
                  Returns None on error
        """
    omop_data = DataMapper().map_x_to_y(synthea_data)
    return omop_data

def load_omop_data_into_database(omop_data: dict):
    """
    Load transformed OMOP data into the OMOP database.

    Args:
        omop_data (dict): A dictionary of DataFrames from transform_to_omop().
    """
    try: 
        omop_db_pw= os.environ.get('OMOP_DB_PASSWORD')
        if omop_db_pw is None:
            raise EnvironmentError("The environment variable 'OMOP_DB_PASSWORD' is not set.")
        # Connect to database
        # Update database.
        # Print out if success/failure
        print("Successfully updated omop")
    except Exception as e:
        print(f"Error in load_omop_data_into_database: An unexpected error occurred: {e}")
    

def synthea_to_omop_etl(data_path: str):
    csv_data = load_synthea_data_from_file(data_path) # Get the data from csv file
    omop_data = transform_synthea_data_to_omop(csv_data) # Map synthea data to omop structure
    load_omop_data_into_database(omop_data) # Connect and update omop database

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Correct usage: python script.py <synthea_data_path>")
        sys.exit(1)
    synthea_data_path = sys.argv[1]
    synthea_to_omop_etl(synthea_data_path)