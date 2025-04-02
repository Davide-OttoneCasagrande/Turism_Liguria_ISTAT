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


def db_execute_SQL_Query(sql_query: str):
    """
    Execute SQL query and return results.
    
    Args:
        sql_query (str): SQL statement.
        
    Returns:
        pandas.DataFrame: Query results as a DataFrame for SELECT queries, 
                          or None for non-SELECT queries.
    """
    print(f"querying DB ... \n{sql_query}")
    
    conn = None
    cursor = None
    df = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if sql_query.strip().upper().startswith('SELECT'):
            #results = cursor.fetchall()
            df = pd.read_sql(sql_query, conn)
        else:
            conn.commit()

        return df
        
    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


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




def save_to_db(df: pd.DataFrame, table_name: str, intId: bool=True) -> None:
    """Save dataframe to database"""   
    try:
        conn = get_db_connection()
        df.to_sql(table_name, connection_string(), if_exists='replace', index=intId)
        print(f"Data successfully saved to {table_name} table")
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
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
    save_to_db(df,tableName,False)

    structure =H_Rest.get_dataStructure(datastructureID)
    print(structure)

    SQL_fKey_script=f"ALTER TABLE public.\"{tableName}\"\n"
    constraint_parts = []

    for codelist in structure['codeList']:
        columns=structure['columns']
        if codelist not in deleted_Columns:
            constraint_parts.append(add_constraint(tableName,columns,codelist))
            if codelist not in ignoreCL:
                df_dimension=H_Rest.get_codelist(codelist)               
                print(f"uploading to db: {dataflow}")
                save_to_db(df_dimension,codelist)
                ignoreCL.append(codelist)

    if constraint_parts:
        # Join all parts and remove the trailing comma
        sql_constraints = ''.join(constraint_parts)
        sql_constraints = sql_constraints.rstrip(',\n') + ";"
        
        # Combine with the ALTER TABLE statement
        SQL_fKey_script += sql_constraints
        
    db_execute_SQL_Query(SQL_fKey_script)
    return ignoreCL


if __name__ == "__main__":
    db_star_schema(ISTAT_var.dataflows[0])