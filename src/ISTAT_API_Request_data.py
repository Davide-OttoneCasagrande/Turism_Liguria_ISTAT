import os
import pandas as pd
import REST_handler as H_Rest

# REST request ISTAT API data
dataType = 0  # 0 = CSV  1 = JSON
maxFilter = 18
timeframe = 'startPeriod=2016-01-01'
searchId:str = "ITC3" #codice per la liguria
dfForGeographicHierarchy:pd.DataFrame = H_Rest.get_codelist("CL_ITTER107")
regionId_Length:int = 4
provinceId_length:int = 5
communeId_lenght:int = 6
filterLocationIdJsonPath = os.path.join("src", "data", "filter_location_id_rest_request.json") #"src//data//filter_location_id_rest_request.json" 
dataflows = [
    ('122_54',"DCSC_TUR","facts_turism"),
    ("68_357","DCCV_TURNOT_CAPI","facts_overnights"),
    ("161_268","DCSP_SBSREG","facts_indicatori_economici")
]


def assign_data(df: pd.DataFrame, filter_condition: bool, id_column: str, columns_to_assign: dict) -> pd.DataFrame:
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
    return df


def process_geographic_hierarchy(df: pd.DataFrame=dfForGeographicHierarchy, searchId: str=searchId) -> pd.DataFrame:

    df['parentId'] = None  # Add columns to the dataframe
    
    province_code_mapping = {}  # Create a mapping of province IDs to their numeric codes
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == regionId_Length) & 
                         (df['id'].str.startswith(searchId))]
    
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        
        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == provinceId_length) & 
                              (df['id'].str.startswith(region_id))]
        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            
            # Set parent_ID for province
            assign_data(df, df['id'] == province_id, 'id', {
                'parentId': region_id
            })
            
            # Find a commune that belongs to this province to get its code
            province_name = province_row['nome']
            sample_communes = df.loc[(df['nome'] == province_name) & 
                                    (df['id'].str.isdigit()) & 
                                    (df['id'].str.len() == communeId_lenght)]
            
            if not sample_communes.empty:
                # Get the first 3 digits of the commune code
                province_code = sample_communes.iloc[0]['id'][:3]
                province_code_mapping[province_id] = province_code
                
                # Find all communes with this province code
                commune_rows = df.loc[df['id'].str.startswith(province_code) & 
                                     (df['id'].str.isdigit()) &
                                     (df['id'].str.len() == communeId_lenght)]
                
                # Set parent_ID for communes
                assign_data(df, df['id'].isin(commune_rows['id']), 'id', {
                    'parentId': province_id
                })
    #display(df)
    return df


def filter(dfCodelist:pd.DataFrame=process_geographic_hierarchy(), maxFilter:int=maxFilter)-> list[str]:
    """
    Args:
        dfCodelist (pd.DataFrame): DataFrame containing the codelist information.
        maxFilter (int): Maximum number of filters for a single REST request (default: 34).

    Returns:
        list[str]: A list of strings, each containing concatenated IDs in the format:
                   "i1+i2+...+in", "in1+in2+...+i2n", suitable for REST requests.
    """

    if "id" not in dfCodelist.columns:
            raise ValueError("DataFrame must contain an 'id' column.")
    fileData= dfCodelist["id"].astype(str)
    
    filters:list[str] = [] 
    i: int = 0

    while i < len(fileData):
        current_ids = fileData[i:i+maxFilter]
        filters.append("+".join(current_ids))
        i+= maxFilter
    return filters

def get_filter_for_dataflow(dataflow:str, filters:list[str]=filter()) -> list[str]:
    """
    Args:
        dataflow (str): The dataflow identifier.
        filters (list[str]): A list of filter strings, each containing concatenated IDs
                             in the format "i1+i2+...+in", suitable for REST requests.

    Returns:
        list[str]: A list of formatted strings specific to the '122_54' dataflow,
                   where each string is wrapped with additional characters for the request.
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
    H_Rest.get_codelist("CL_ITTER107")

    filterData: list[str] = filter()
    print(filterData)