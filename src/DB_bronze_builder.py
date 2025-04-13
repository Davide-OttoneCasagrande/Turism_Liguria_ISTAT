import pandas as pd
import psycopg2
import REST_handler as RestH
import global_VAR as gVAR
import ISTAT_API_Request_data as ISTAT_var
from dotenv import load_dotenv
from os import getenv

load_dotenv()

def connection_string() -> str:
    """
    Create a PostgreSQL connection string using environment variables.
    
    Returns:
        str: Formatted PostgreSQL connection string.
    """
    return f"postgresql://{getenv('User')}:{getenv('password')}@{getenv('host')}:{getenv('port')}/{getenv('database')}"

def get_db_connection() -> psycopg2.extensions.connection:
    """
    Create and return a database connection.
    
    Returns:
        psycopg2.extensions.connection: PostgreSQL database connection object.
    """
    return psycopg2.connect(
        dbname=getenv('database'),
        user=getenv('User'),
        password=getenv('password'),
        host=getenv('host'),
        port=getenv('port')
    )

def save_to_db(df: pd.DataFrame, table_name: str, intIndex: bool=True) -> None:
    """
    Save dataframe to database.
    
    Args:
        df (pd.DataFrame): DataFrame to save to the database.
        table_name (str): Name of the table to save the data to.
        intIndex (bool, optional): Whether to include the index in the table. Defaults to True.
    """  
    try:
        conn = get_db_connection()
        df.to_sql(table_name, connection_string(), if_exists='replace', index=intIndex)
        print(f"Data successfully saved to {table_name}")
    except Exception as e:
        print(f"Error saving to database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        conn.close()

def db_insert_fact(dataflowID: str, tableName: str, filters: list[str]) -> list[str]:
    """
    Process and insert fact data into database.
    
    Args:
        dataflowID (str): ID of the dataflow to retrieve.
        tableName (str): Name of the table to save the data to.
        filters (list[str]): List of filters to apply to the dataflow.
        
    Returns:
        list[str]: List of deleted columns (empty or constant valued).
    """
    df = RestH.get_dataflow(dataflowID, filters, gVAR.timeframe)
    deleted_Columns = df.columns[df.isna().all()].tolist()
    df = df.dropna(axis=1, how='all')
    constant_columns = df.columns[df.nunique() == 1].tolist()
    deleted_Columns.extend(constant_columns)
    df = df.drop(columns=constant_columns)
    print(f"uploading to db: {dataflowID}")
    save_to_db(df, tableName, False)
    return deleted_Columns

def db_insert_dim(datastructureID: str, ignoreCL: list[str]=None) -> tuple:
    """
    Process and insert dimension data into database.
    
    Args:
        datastructureID (str): ID of the data structure to retrieve.
        ignoreCL (list[str], optional): List of codelists to ignore. Defaults to None.
        
    Returns:
        tuple: Updated list of codelists to ignore and structure data.
    """
    if ignoreCL is None:
        ignoreCL = []
    structure = RestH.get_dataStructure(datastructureID)
    for index, row in structure.iterrows():
        codelist = row['codeList'].upper()
        if codelist not in ignoreCL:
            df_dimension = RestH.get_codelist(codelist)              
            print(f"uploading to db: {codelist}")
            save_to_db(df_dimension, codelist)
            ignoreCL.append(codelist)
    return ignoreCL, structure

def db_star_schema(dataflow: tuple, ignoreCL: list = None) -> list[str]:
    """
    Create a star schema database structure for ISTAT data.
    
    Args:
        dataflow (tuple): A tuple containing (dataflowID, datastructureID, tableName).
        ignoreCL (list, optional): List of codelists to ignore. Defaults to None.
        
    Returns:
        list[str]: Updated list of codelists to ignore.
    """
    dataflowID, datastructureID, tableName = dataflow
    location_struct = ISTAT_var.filter()
    filters, dim_geography = location_struct
    
    if gVAR.originFilterSTR not in ignoreCL:
        ignoreCL.append(gVAR.originFilterSTR)
        save_to_db(dim_geography, 'CL_LOCATION_HIERARCHY')
    
    filters_for_fact = ISTAT_var.get_filter_for_dataflow(dataflowID, filters)
    deleted_Columns = db_insert_fact(dataflowID, tableName, filters_for_fact)
    ignoreCL.append(deleted_Columns)
    
    structure = db_insert_dim(datastructureID, ignoreCL)
    print(structure[1])
    ignoreCL = structure[0]
    print(f"{tableName} star schema created")
    return ignoreCL