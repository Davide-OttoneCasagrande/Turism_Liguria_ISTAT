import os
import DB_bronze_builder as bronze

def execute_sql_query(sql_query: str) -> None:
    """
    Execute SQL query that creates or replaces views.
   
    Args:
        sql_query (str): SQL statement for creating/replacing views.
    """
    print(f"Executing view creation...\n{sql_query[:100]}...")  # Print just first 100 chars for brevity
    conn = None
    cursor = None
   
    try:
        conn = bronze.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()
        print("View created or replaced successfully")
            
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

def sql_query_from_file(sql_filepath: str) -> None:
    """
    Execute SQL script from file to create or replace views
    
    Args:
        sql_filepath (str): the path to the SQL script file
    """
    with open(sql_filepath, 'r') as file:
        sql_query = file.read()
    execute_sql_query(sql_query)

def process_sql_files(folder_path: str) -> None:
    """
    Process all SQL files in a given folder to create or replace views
    
    Args:
        folder_path (str): Path to the folder containing SQL files

    Raises:
        Exception: If any database error occurs during execution
    """
    if not os.path.exists(folder_path):
        print(f"Folder not found: {folder_path}")
        return
        
    sql_files = [f for f in os.listdir(folder_path) if f.endswith('.sql')]
    
    if not sql_files:
        print(f"No SQL files found in {folder_path}")
        return
        
    print(f"Found {len(sql_files)} SQL files to process")
    
    for sql_file in sql_files:
        file_path = os.path.join(folder_path, sql_file)
        print(f"\nExecuting file: {sql_file}")
        sql_query_from_file(file_path)