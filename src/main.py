import os
import sys
import log_handler as log
import global_VAR as gVAR
import DB_builder as DB_b

def validate_folder_path(log, folder_path: dict[str, str]) -> str:
    """
    Validates that the provided folder path exists on the filesystem.

    This function checks if the path constructed from the given dictionary exists.
    If it does not exist, a FileNotFoundError is raised, providing details about the folder.

    Args:
        log (logging.Logger): Logger instance used for logging errors.
        folder_path (dict[str, str]): A dictionary with keys 'root', 'path', and 'name' representing the folder structure.

    Returns:
        str: The full path to the folder if it exists.

    Raises:
        FileNotFoundError: Raised if the constructed folder path does not exist.
    """
    path = os.path.join(folder_path["root"], folder_path["path"])
    full_path = os.path.join(path, folder_path["name"])
    if not os.path.exists(full_path):
        log.error(f"The folder '{folder_path['name']}' at path '{path}' does not exist.")
        raise FileNotFoundError(f"The folder '{folder_path['name']}' at path '{path}' does not exist.")
    return full_path

def main() -> None:
    """
    Main function to create and populate the first and second database layers.
    
    This function orchestrates the entire ETL process by:
    1. Creating the first layer database tables using star schema design for each dataflow
    2. Processing SQL files to create views in the second database layer:
       - Fact views that represent business processes
       - Dimension views that represent business entities
    
    The function handles errors at both first and second layers by terminating execution
    if any database error occurs during the process.
    
    Returns:
        None
    
    Raises:
        SystemExit: If an error occurs during database creation or view processing
    """
    logger = log.setup_logging()
    logger.info("Starting database build process")
    ignoreCL: list[str] = []
    try:
     # Create first layer DB
        logger.info(f"Creating first DB layer for {len(gVAR.dataflows)} dataflows")
        sql_script_path = validate_folder_path(logger, gVAR.drop_script_path)
        DB_b.sql_query_from_file(logger, sql_script_path)
        for dataflow in gVAR.dataflows:
            logger.info(f"Processing dataflow: {dataflow}")
            ignoreCL = DB_b.db_star_schema(logger, dataflow, ignoreCL)
        logger.info("First DB layer created successfully")
        
     # Create second layer DB
        # Process fact views
        logger.info("Creating second DB layer")
        logger.info("\n=== Processing fact views ===")
        sql_script_path = validate_folder_path(logger, gVAR.facts_FolderPath)
        DB_b.process_sql_files(logger, sql_script_path)
        logger.info("Fact views processed successfully")

        # Process dimension views
        logger.info("\n=== Processing dimension views ===")
        sql_script_path = validate_folder_path(logger, gVAR.dim_FolderPath)
        DB_b.process_sql_files(logger, sql_script_path)
        logger.info("Dimension views processed successfully")

        logger.info("Database build completed successfully")
        
    except Exception as e:
        logger.error(f"FATAL ERROR: {str(e)}", exc_info=True)
        sys.exit(1)  # Exit with error code

if __name__ == "__main__":
    main()