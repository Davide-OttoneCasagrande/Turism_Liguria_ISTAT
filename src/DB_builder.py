import os
import pandas as pd
import psycopg2
import REST_handler as H_Rest
import ISTAT_API_Request_data as ISTAT_var
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()


def connection_string() -> str:
    """
        Create a PostgreSQL connection string using environment variables.
        
        Returns:
            str: Formatted PostgreSQL connection string.
    """
    return f"postgresql://{os.getenv('User')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"

def get_engine():
    """
        Create a SQLAlchemy engine for database connections.
        
        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy database engine.
    """
    return create_engine(connection_string())


def add_constraint(tableName:str,columnsName:str,codeList:str)->str:
    """
        Generate SQL for adding a foreign key constraint.
        
        Args:
            tableName (str): Name of the table receiving the constraint.
            columnName (str): Name of the column to constrain.
            codeList (str): Table name the column references.
            
        Returns:
            str: SQL fragment for adding the foreign key constraint.
    """
    return f"ADD CONSTRAINT {tableName}_{columnsName}_{codeList} FOREIGN KEY(\"{columnsName}\") REFERENCES public.{codeList}(id),\n"


def db_execute_SQL_Query(sql_query:str):
    """
        Execute SQL query.
        
        Args:
            sql_query (str): SQL statement.
    """
    print(f"quering DB ... \n{sql_query}")

    conn: psycopg2.extensions.connection = psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('User'),
        password=os.getenv('password'),
        host=os.getenv('host'),
        port=os.getenv('port')
    ) 
    cursor = conn.cursor()
    cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    conn.close()


def db_star_schema(dataflow: tuple, ignoreCL: list = None) -> list[str]: #ToDo test this
    """
        Create a star schema database structure for ISTAT data.
        
        Args:
            dataflow (tuple): A tuple containing (dataflowID, datastructureID, tableName).
            ignoreCL (list, optional): List of codelists to ignore. Defaults to empty list.
            
        Returns:
            list: Updated list of codelists to ignore.
    """
    if ignoreCL is None:
        ignoreCL = []

    dataflowID, datastructureID, tableName = dataflow

    df = H_Rest.get_dataflow(dataflowID)
    deleted_Columns = df.columns[df.isna().all()].tolist()
    df = df.dropna(axis=1, how='all')
    constant_columns = df.columns[df.nunique() == 1].tolist()
    deleted_Columns.extend(constant_columns)
    df = df.drop(columns=constant_columns)

    print(f"uploading to db: {dataflow}")
    df.to_sql(tableName, get_engine(), if_exists='replace', index=False)

    structure =H_Rest.get_dataStructure(datastructureID)
    print(structure)

    SQL_fKey_script=f"ALTER TABLE public.\"{tableName}\"\n"
    constraint_parts = []

    for codelist in structure['codeList']:
        if codelist not in deleted_Columns:
            constraint_parts.append(add_constraint(tableName,structure['columns'],codelist))
            if codelist not in ignoreCL:
                df_dimension=H_Rest.get_codelist(codelist)               
                print(f"uploading to db: {dataflow}")
                df_dimension.to_sql(codelist, get_engine(), if_exists='replace', index=True)
                ignoreCL.append(codelist)

    if constraint_parts:
        # Join all parts and remove the trailing comma
        sql_constraints = ''.join(constraint_parts)
        sql_constraints = sql_constraints.rstrip(',\n') + ";"
        
        # Combine with the ALTER TABLE statement
        SQL_fKey_script += sql_constraints
        
    db_execute_SQL_Query(SQL_fKey_script)
    return ignoreCL

def build_location_Hierarcy(): #TODO implement this

    return

if __name__ == "__main__":
    db_star_schema(ISTAT_var.dataflows[0])