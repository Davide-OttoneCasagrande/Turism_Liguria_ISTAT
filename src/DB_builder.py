import os
import pandas as pd
import psycopg2
import REST_handler as H_Rest
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
    empty_Columns = df.columns[df.isna().all()].tolist()
    df = df.dropna(axis=1, how='all')

    df.to_sql(tableName, get_engine(), if_exists='replace', index=False)

    structure =pd.json_normalize(H_Rest.get_dataStructure(datastructureID))
    structure.columns = ['columns', 'codeList']
    print(structure)

    SQL_fKey_script=f"ALTER TABLE public.\"{tableName}\"\n"
    constraint_parts = []

    for codelist in structure['codeList']:
        if codelist not in empty_Columns:
            constraint_parts.append(add_constraint(tableName,structure['columns'],codelist))
            if codelist not in ignoreCL:
                df_dimension=H_Rest.get_codelist(codelist)
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