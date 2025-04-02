import pandas as pd
import psycopg2
import REST_handler as H_Rest
import ISTAT_API_Request_data as ISTAT_var
import DB_bronze_builder as bronze
from dotenv import load_dotenv
from os import getenv
load_dotenv()


searchId:str = "IT"
regionId_Length:int = 4
provinceId_length:int = 5
communeId_lenght:int = 6


def sql_query_from_file(sql_filePath) -> pd.DataFrame:
    """
        execute SQL script using SQL file
        Args:
            sql_filePath (str): the path to the SQL script file
            
        Returns:
           pd.DataFrame (optional) : if the script is a SELECT the dataframe of the selected table
    """
    
    with open(sql_filePath, 'r') as file:   # Read the SQL file
        sql_query = file.read()
    df=bronze.db_execute_SQL_Query(sql_query)
    return df


def assign_data(df: pd.DataFrame, filter_condition: bool, id_column: str, columns_to_assign: dict) -> pd.DataFrame:
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
    return df


def process_geographic_hierarchy(df: pd.DataFrame, searchId: str=searchId) -> pd.DataFrame:

    df['parentId'] = None  # Add columns to the dataframe
    
    province_code_mapping = {}  # Create a mapping of province IDs to their numeric codes
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == regionId_Length) & 
                         (df['id'].str.startswith(searchId))]
    
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        
        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == provinceId_length + 1) & 
                              (df['id'].str.startswith(region_id))]
        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            
            # Set parent_ID for province
            assign_data(df, df['id'] == province_id, 'id', {
                'parentId': region_id
            })
            
            # Find a commune that belongs to this province to get its code
            province_name = province_row['nome']
            sample_communes = df.loc[(df['nome'] == province_name) & 
                                    (df['id'].str.isdigit()) & 
                                    (df['id'].str.len() == communeId_lenght)]
            
            if not sample_communes.empty:
                # Get the first 3 digits of the commune code
                province_code = sample_communes.iloc[0]['id'][:3]
                province_code_mapping[province_id] = province_code
                
                # Find all communes with this province code
                commune_rows = df.loc[df['id'].str.startswith(province_code) & 
                                     (df['id'].str.isdigit()) &
                                     (df['id'].str.len() == communeId_lenght)]
                
                # Set parent_ID for communes
                assign_data(df, df['id'].isin(commune_rows['id']), 'id', {
                    'parentId': province_id
                })
    #display(df)
    return df

def build_location_Hierarcy(pathSQL_request: str) -> None:
    searchId = 'ITC' # Starting search string
    df = sql_query_from_file(pathSQL_request)
    df = process_geographic_hierarchy(df,searchId)
    
    print(f"Processed {df['parentId'].notna().sum()} entry")   # Print summary
    bronze.save_to_db(df, "dim_location_Hierarcy")