import os
import json

# REST request ISTAT API data
datatype = 0  # 0 = CSV  1 = JSON
dataflow = '122_54'
timeframe = 'startPeriod=2016-01-01'
maxFilter = 35
filterLocationIdJsonPath = os.path.join("data", "filter_location_id_rest_request.json")

def filter(locationID_path:str=filterLocationIdJsonPath, maxFilter:int=maxFilter)-> list[str]:

    with open(locationID_path,"r") as file:
        data = json.load(file)
    fileData= data["id"]
    
    filters:list[str] = [] 
    current_filter =f"{fileData[0]}"
    i: int = 1
    for I in range(len(fileData)-1):
        I=I+1
        if i <= maxFilter-1:
            current_filter= f"{current_filter}+{fileData[I]}"
            i=i+1
        if i==maxFilter:
            filters.append(current_filter)
            i=0
            current_filter= f"{fileData[I]}"
    return filters

def filter_for_122_54(filters:list[str]=filter()) -> list[str]:
    FilterString122_54:list[str] = []
    for filter in filters:
        FilterString122_54.append(f"...{filter}.......")
    return FilterString122_54

if __name__ == '__main__':
    filterData: list[str] = filter()
    print(filterData)