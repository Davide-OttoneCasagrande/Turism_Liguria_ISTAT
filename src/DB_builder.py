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
    df=db_execute_SQL_Query(sql_query)
    return df


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


def assign_data(df: pd.DataFrame, filter_condition: bool, id_column: str, columns_to_assign: dict) -> pd.DataFrame:
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
    return df


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
        if codelist not in deleted_Columns:
            constraint_parts.append(add_constraint(tableName,structure['columns'],codelist))
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


def process_geographic_hierarchy(df: pd.DataFrame, searchId: str) -> pd.DataFrame:

    df['parent_ID'] = None  # Add columns to the dataframe
    
    province_code_mapping = {}  # Create a mapping of province IDs to their numeric codes
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == len(searchId) + 1) & 
                         (df['id'].str.startswith(searchId))]
    
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        
        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == len(region_id) + 1) & 
                              (df['id'].str.startswith(region_id))]
        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            
            # Set parent_ID for province
            assign_data(df, df['id'] == province_id, 'id', {
                'parent_ID': region_id
            })
            
            # Find a commune that belongs to this province to get its code
            province_name = province_row['nome']
            sample_communes = df.loc[(df['nome'] == province_name) & 
                                    (df['id'].str.isdigit()) & 
                                    (df['id'].str.len() == 6)]
            
            if not sample_communes.empty:
                # Get the first 3 digits of the commune code
                province_code = sample_communes.iloc[0]['id'][:3]
                province_code_mapping[province_id] = province_code
                
                # Find all communes with this province code
                commune_rows = df.loc[df['id'].str.startswith(province_code) & 
                                     (df['id'].str.isdigit()) &
                                     (df['id'].str.len() == 6)]
                
                # Set parent_ID for communes
                assign_data(df, df['id'].isin(commune_rows['id']), 'id', {
                    'parent_ID': province_id
                })
    #display(df)
    return df


def build_location_Hierarcy(pathSQL_request: str) -> None:
    searchId = 'ITC' # Starting search string
    df = sql_query_from_file(pathSQL_request)
    df = process_geographic_hierarchy(df,searchId)
    
    print(f"Processed {df['parent_ID'].notna().sum()} entry")   # Print summary
    save_to_db(df, "gerarchia_luogo")



if __name__ == "__main__":
    db_star_schema(ISTAT_var.dataflows[0])