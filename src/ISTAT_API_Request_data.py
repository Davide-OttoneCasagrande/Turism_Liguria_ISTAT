import os
import json

# REST request ISTAT API data
dataType = 0  # 0 = CSV  1 = JSON
maxFilter = 34
timeframe = 'startPeriod=2016-01-01'
filterLocationIdJsonPath = os.path.join("src", "data", "filter_location_id_rest_request.json") #"src//data//filter_location_id_rest_request.json" 
dataflows = [
    ('122_54',"DCSC_TUR","facts_turism"),
    ("68_357","DCCV_TURNOT_CAPI","facts_overnights"),
    ("161_268","DCSP_SBSREG","facts_indicatori_economici")
]


def filter(locationID_path:str=filterLocationIdJsonPath, maxFilter:int=maxFilter)-> list[str]:
    """
    Args:
        locationID_path (str): Path to the JSON file with the structure: {"id": [i1, i2, ...]}.
        maxFilter (int): Maximum number of filters for a single REST request (default: 34).

    Returns:
        list[str]: A list of strings, each containing concatenated IDs in the format:
                   "i1+i2+...+in", "in1+in2+...+i2n", suitable for REST requests.
    """

    with open(locationID_path,"r") as file:
        data = json.load(file)
    fileData= data["id"]
    
    filters:list[str] = [] 
    i: int = 0

    while i < len(fileData):
        current_slice = fileData[i:i+maxFilter]
        filters.append("+".join(current_slice))
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