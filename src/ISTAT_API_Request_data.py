import os
import pandas as pd
import REST_handler as Rest_H

# REST request ISTAT API data
dataType = 0  # 0 = CSV  1 = JSON
maxFilter = 18
timeframe = 'startPeriod=2016-01-01'
searchId:str = "ITC3" #codice per la liguria
originFilterSTR:str = "CL_ITTER107"
regionId_Length:int = 4
provinceId_length:int = 5
communeId_lenght:int = 6
dataflows = [
    ('122_54',"DCSC_TUR","facts_turism"),
    ("161_268","DCSP_SBSREG","facts_indicatori_economici")
]


def assign_data(df: pd.DataFrame, filtered_df:pd.DataFrame, parent_id:str=None) -> pd.DataFrame:
    for _, row in filtered_df.iterrows():
            id = row['id']
            nome = row['nome']
            name = row['name']
            # concat df
            df=pd.concat([df, pd.DataFrame({
                'id': [id],
                'parent_id': [parent_id],
                'nome': [nome],
                'name': [name]
            })], ignore_index=True)  
    return df


def process_geographic_hierarchy(CodelistName:str = originFilterSTR, searchId: str=searchId) -> pd.DataFrame:

    df = Rest_H.get_codelist(CodelistName) 
    Hierarcy_df = pd.DataFrame(columns=['id', 'parent_id', 'nome', 'name'])
    
    province_code_mapping = {}  # Create a mapping of province IDs to their numeric codes
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == regionId_Length) & 
                         (df['id'].str.startswith(searchId))]
    
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        region_nome = region_row['nome']
        region_name = region_row['name']
        Hierarcy_df = pd.concat([Hierarcy_df, pd.DataFrame({
            'id': region_id,
            'parent_id': None,
            'nome': region_nome,
            'name': region_name
        })], ignore_index=True)
        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == provinceId_length) & 
                              (df['id'].str.startswith(region_id))]
        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            province_nome = province_row['nome']
            province_name = province_row['name']
            # Set parent_ID for province
            Hierarcy_df = pd.concat([Hierarcy_df, pd.DataFrame({
                'id': province_id,
                'parent_id': region_id,
                'nome':province_nome,
                'name':province_name
            })], ignore_index=True)
            
            # Find a commune that belongs to this province to get its code
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
                
                for _, commune_row in commune_rows.iterrows():
                    commune_id = commune_row['id']
                    commune_nome = commune_row['nome']
                    commune_name = commune_row['name']
                    # Set parent_ID for communes
                    Hierarcy_df = pd.concat([Hierarcy_df, pd.DataFrame({
                    'id': commune_id,
                    'parent_id': province_id,
                    'nome':commune_nome,
                    'name':commune_name
            })], ignore_index=True)
    return Hierarcy_df


def filter(maxFilter:int=maxFilter)-> list[str]:
    """
    Arg:
        maxFilter (int): Maximum number of filters for a single REST request (default: 34).

    Returns:
        list[str]: A list of strings, each containing concatenated IDs in the format:
                   "i1+i2+...+in", "in1+in2+...+i2n", suitable for REST requests.
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
    filterData: list[str] = filter()
    print(filterData)