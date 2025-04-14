import sys
import global_VAR as gVAR
import DB_builder as DB_b
import os

def validate_folder_path(folder_path: dict[str, str]) -> str:
    """
    Validates that the provided folder path exists on the filesystem.

    This function checks if the path constructed from the given dictionary exists.
    If it does not exist, a FileNotFoundError is raised, providing details about the folder.

    Args:
        folder_path (dict[str, str]): A dictionary with keys 'root', 'path', and 'name' representing the folder structure.

    Returns:
        str: The full path to the folder if it exists.

    Raises:
        FileNotFoundError: Raised if the constructed folder path does not exist.
    """
    path = os.path.join(folder_path["root"], folder_path["path"])
    full_path = os.path.join(path, folder_path["name"])
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"The folder '{folder_path['name']}' at path '{path}' does not exist.")
    return full_path

def main() -> None:
    """
    Main function to create and populate the bronze and silver database layers.
    
    This function orchestrates the entire ETL process by:
    1. Creating the bronze database tables using star schema design for each dataflow
    2. Processing SQL files to create views in the silver database layer:
       - Fact views that represent business processes
       - Dimension views that represent business entities
    
    The function handles errors at both bronze and silver layers by terminating execution
    if any database error occurs during the process.
    
    Returns:
        None
    
    Raises:
        SystemExit: If an error occurs during database creation or view processing
    """
    ignoreCL: list[str] = []
    try:
        # Create bronze DB
        sql_script_path = validate_folder_path(gVAR.drop_script_path)
        DB_b.sql_query_from_file(sql_script_path)
        for dataflow in gVAR.dataflows:
            ignoreCL = DB_b.db_star_schema(dataflow, ignoreCL)
        print("Bronze DB created")
        
        # Create silver DB
        # Process fact views
        print("\n=== Processing fact views ===")
        sql_script_path = validate_folder_path(gVAR.facts_FolderPath)
        DB_b.process_sql_files(sql_script_path)
        
        # Process dimension views
        print("\n=== Processing dimension views ===")
        sql_script_path = validate_folder_path(gVAR.dim_FolderPath)
        DB_b.process_sql_files(sql_script_path)
        
        print("\nAll view creation scripts processed successfully")
        print("\nDatabase successfully compiled")
        
    except Exception as e:
        print(f"\n!!! FATAL ERROR: Script execution terminated !!!")
        print(f"Error details: {e}")
        sys.exit(1)  # Exit with error code

if __name__ == "__main__":
    main()