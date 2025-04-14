import pandas as pd
import global_VAR as gVAR
import REST_handler as Rest_H

def process_geographic_hierarchy(CodelistName:str = gVAR.originFilterSTR, searchId: str = gVAR.searchId) -> pd.DataFrame:
    """
    Processes geographic data to create a hierarchical structure of regions, provinces, and communes.
    Uses a REST API to fetch geographic data and organizes it into a hierarchy where:
    - Regions are top-level entities
    - Provinces belong to regions
    - Communes belong to provinces
    
    Args:
        CodelistName (str): The name of the codelist to retrieve from the REST API
                        (default: value from global_VAR.originFilterSTR)
        searchId (str): The ID prefix to filter regions by 
                    (default: value from global_VAR.searchId)
    
    Returns:
        pd.DataFrame: A DataFrame containing hierarchical geographic data with columns:
                    'id', 'parent_id', 'nome' (Italian name), and 'name' (English name)
    """

    df = Rest_H.get_codelist(CodelistName)
    hierarchy_data = []
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == gVAR.ID_lengths["region"]) & 
                         (df['id'].str.startswith(searchId))]
    for _, region_row in region_rows.iterrows():
        region_id:str = region_row['id']
        # compile Hierarcy_data with the region
        hierarchy_data.append({
            'id': region_id,
            'parent_id': '',
            'nome': region_row['nome'],
            'name': region_row['name']
        })

        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == gVAR.ID_lengths["province"]) & 
                              (df['id'].str.startswith(region_id))]        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            province_IT_name = province_row['nome']
            # compile Hierarcy_data with the provinces
            hierarchy_data.append({
                'id': province_id,
                'parent_id': region_id,
                'nome': province_IT_name,
                'name': province_row['name']
            })
            
            # Step 3: Process communes
            # Find a commune that belongs to this province to get its code
            sample_communes = df.loc[(df['nome'] == province_IT_name) & 
                                    (df['id'].str.isdigit()) & 
                                    (df['id'].str.len() == gVAR.ID_lengths["commune"])]
            if not sample_communes.empty:
                # Get the first 3 digits of the commune code
                province_code = sample_communes.iloc[0]['id'][:3]
                # Find all communes with this province code
                commune_rows = df.loc[df['id'].str.startswith(province_code) & 
                                     (df['id'].str.isdigit()) &
                                     (df['id'].str.len() == gVAR.ID_lengths["commune"])]
                
                for _, commune_row in commune_rows.iterrows():
                    # compile Hierarcy_data with the provinces
                    hierarchy_data.append({
                        'id': commune_row['id'],
                        'parent_id': province_id,
                        'nome': commune_row['nome'],
                        'name': commune_row['name']
                    })
    return pd.DataFrame(hierarchy_data)

def filter(maxFilter:int = gVAR.max_num_filter)-> tuple [list[str],pd.DataFrame]:
    """
    Processes geographic hierarchy data and creates filter strings for REST API requests.
    This function divides all geographic IDs into chunks to prevent exceeding API request limits.
    
    Args:
        maxFilter (int): Maximum number of IDs to include in a single filter string
                        (default: value from global_VAR.max_num_filter)
    
    Returns:
        tuple: Contains:
            - list[str]: Filter strings, each containing concatenated IDs in the format "id1+id2+...+idN"
            - pd.DataFrame: The complete geographic hierarchy data
    
    Raises:
        ValueError: If the processed DataFrame doesn't contain an 'id' column
    """

    dfCodelist = process_geographic_hierarchy()

    if "id" not in dfCodelist.columns:
            raise ValueError("DataFrame must contain an 'id' column.")
    fileData= dfCodelist["id"].astype(str)
    
    filters:list[str] = [] 
    i: int = 0

    while i < len(fileData):
        current_ids = fileData[i:i+maxFilter]
        filters.append("+".join(current_ids))
        i+= maxFilter
    return filters, dfCodelist

def get_filter_for_dataflow(dataflow:str, filters:list[str]) -> list[str]:
    """
    Formats filter strings based on specific dataflow requirements.
    Different dataflows require different formatting of the filter strings.
    This function applies the appropriate formatting based on the dataflow ID.
    
    Args:
        dataflow (str): The dataflow identifier (e.g., "122_54", "68_357", "161_268")
        filters (list[str]): A list of filter strings, each containing concatenated IDs
                        (it should be the output from the filter() function)
    
    Returns:
        list[str]: A list of formatted filter strings specific to the requested dataflow
    
    Raises:
        ValueError: If an unknown dataflow ID is provided
    """
    FilterString:list[str] = []
    
    if dataflow == "122_54":
        for filter in filters:
            FilterString.append(f".{filter}.........")
    elif dataflow == "68_357":
        for filter in filters:
            FilterString.append(f"...{filter}.............")
    elif dataflow == "161_268":
        for filter in filters:
            FilterString.append(f".{filter}.........")
    else:
        raise ValueError(f"Unknown dataflow: {dataflow}. Please check the dataflow ID.")
    return FilterString

if __name__ == '__main__':
    filterData: list[str] = filter()
    print(filterData)