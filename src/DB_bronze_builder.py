import pandas as pd
import psycopg2
import REST_handler as H_Rest
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
    """Create and return a database connection"""
    return psycopg2.connect(
        dbname = getenv('database'),
        user = getenv('User'),
        password = getenv('password'),
        host = getenv('host'),
        port = getenv('port')
    )


def save_to_db(df: pd.DataFrame, table_name: str, intIndex: bool=True) -> None:
    """Save dataframe to database"""   
    try:
        conn = get_db_connection()
        df.to_sql(table_name, connection_string(), if_exists='replace', index=intIndex)
        print(f"Data successfully saved to {table_name}")
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        conn.close()


def db_insert_fact(dataflowID:str,tableName:str)->list[str]:
    df = H_Rest.get_dataflow(dataflowID)
    deleted_Columns = df.columns[df.isna().all()].tolist()
    df = df.dropna(axis=1, how='all')
    constant_columns = df.columns[df.nunique() == 1].tolist()
    deleted_Columns.extend(constant_columns)
    df = df.drop(columns=constant_columns)
    print(f"uploading to db: {dataflowID}")
    save_to_db(df,tableName,False)
    return deleted_Columns


def db_insert_dim(datastructureID:str, ignoreCL:list[str]=None)->tuple:
    if ignoreCL is None:
        ignoreCL = []
    structure =H_Rest.get_dataStructure(datastructureID)
    for index, row in structure.iterrows():
        codelist = row['codeList'].upper()
        if codelist not in ignoreCL:
            df_dimension=H_Rest.get_codelist(codelist)               
            print(f"uploading to db: {codelist}")
            save_to_db(df_dimension,codelist)
            ignoreCL.append(codelist)
    return ignoreCL,structure


def db_star_schema(dataflow: tuple, ignoreCL: list = None) -> list[str]:
    """
        Create a star schema database structure for ISTAT data.
        
        Args:
            dataflow (tuple): A tuple containing (dataflowID, datastructureID, tableName).
            ignoreCL (list, optional): List of codelists to ignore. Defaults to empty list.
            
        Returns:
            list: Updated list of codelists to ignore.
    """
    dataflowID, datastructureID, tableName = dataflow
    deleted_Columns = db_insert_fact(dataflowID,tableName)
    ignoreCL.append(deleted_Columns)
    structure=db_insert_dim(datastructureID,ignoreCL)
    print(structure[1])
    ignoreCL=structure[0]
    return ignoreCL



def insert_location_Hierarcy(dfLocationFilterCodeList:pd.DataFrame) -> str:
    print(f"Processed {dfLocationFilterCodeList['parentId'].notna().sum()} entry")   # Print summary
    save_to_db(dfLocationFilterCodeList, "dim_location_Hierarcy",False)
    return  "CL_ITTER107"


if __name__ == "__main__":
    ignoreCL:list[str] = []
    fstCL = ISTAT_var.process_geographic_hierarchy(H_Rest.get_codelist("CL_ITTER107"))
    ignoreCL.append(insert_location_Hierarcy(fstCL))
    for dataflow in ISTAT_var.dataflows:
        ignoreCL=db_star_schema(dataflow,ignoreCL)