import requests as req
import pandas as pd
import io
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
load_dotenv()

def get_dataflow(dataflow:str, filters:list[str], timeframe:str) -> pd.DataFrame:
    """
    Fetch data from the ISTAT API.

    Args:
        dataflow (str): The dataflow identifier.
        filters (list[str]): List of filter identifiers to apply to the request.
        timeframe (str): The timeframe for the request.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data.

    Raises:
        RuntimeError: If the HTTP request fails with a status code other than 404.
        ValueError: If no data is returned from the API.
    """
     
    dfs = []

    print(f"downloading dataflow: {dataflow}")

    for filterID in filters:
        url =  f'https://esploradati.istat.it/SDMXWS/rest/data/{dataflow}/{filterID}?{timeframe}'
        headerCSV = {'Accept': 'text/csv'}
        try:
            response = req.get(url, headers=headerCSV)
            response.raise_for_status()
            df= pd.read_csv(io.StringIO(response.text))
            dfs.append(df)
        except req.exceptions.HTTPError as e:
            if response.status_code == 404:
                print(f"Request failed code: {e}")
            else:
                print(f"Request failed code: {e}")
                raise RuntimeError(f"Request failed code: {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        raise ValueError("No data returned from the API.")


def get_codelist(codeListName: str) -> pd.DataFrame:
    """
    Fetch the codelist from the ISTAT API.

    Args:
        codeListName (str): The codelist identifier.

    Returns:
        pd.DataFrame: A DataFrame containing the codelist with columns:
            - id: code identifiers
            - name: code names in English
            - nome: code names in Italian

    Notes:
        If an error occurs during the request, an empty DataFrame with the expected columns is returned.
    """
    url = f'https://esploradati.istat.it/SDMXWS/rest/codelist/IT1/{codeListName}'

    print(f"downloading codelist: {codeListName}")
    try:
        codeList_Response = req.get(url, timeout=20)
        codeList_Response.raise_for_status()
        codeList_XML = ET.fromstring(codeList_Response.content)
        
        namespaces = {
            "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
            "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
        }

        code_ids = []
        code_names_en = []
        code_names_it = []

        # Find all Code elements under the Codelist
        for code in codeList_XML.findall('.//structure:Code', namespaces):
            code_id = code.get('id')
            
            # Get English name
            name_en = code.find('./common:Name[@xml:lang="en"]', 
                               {**namespaces, 'xml': 'http://www.w3.org/XML/1998/namespace'})
            
            # Get Italian name
            name_it = code.find('./common:Name[@xml:lang="it"]', 
                               {**namespaces, 'xml': 'http://www.w3.org/XML/1998/namespace'})
            
            if code_id is not None:
                code_ids.append(code_id)
                code_names_en.append(name_en.text if name_en is not None else "")
                code_names_it.append(name_it.text if name_it is not None else "")

        # Create DataFrame with the collected data
        codelist_df = pd.DataFrame({
            'id': code_ids,
            'name': code_names_en,
            'nome': code_names_it
        })
        
        return codelist_df
        
    except req.exceptions.RequestException as e:
        print(f"Error retrieving codelist: {e}")
        return pd.DataFrame(columns=['id', 'name', 'nome'])


def get_dataStructure(dataflowID:str) -> pd.DataFrame:
    """
    Fetch and parse the data structure for a given dataflow ID from the ISTAT SDMX web service.

    Args:
        dataflowID (str): The identifier of the dataflow for which the data structure is to be retrieved.

    Returns:
        pd.DataFrame: A DataFrame with columns:
            - columns: dimension IDs
            - codeList: corresponding codelist IDs
            Returns an empty DataFrame with these columns if an error occurs

    Raises:
        No exceptions are raised directly as they are caught and logged.
        Underlying exceptions that might occur include:
            - requests.exceptions.RequestException: If there is an issue with the HTTP request
            - xml.etree.ElementTree.ParseError: If there is an issue parsing the XML response

    Examples:
        >>> get_dataStructure('exampleDataflowID')
        columns   codeList
        0    DIM1    CL_DIM1
        1    DIM2    CL_DIM2
    """
    print(f"downloading datastructure: {dataflowID}")
    url:str = f'https://esploradati.istat.it/SDMXWS/rest/datastructure/IT1/{dataflowID}'

    try:
        structureResponse = req.get(url, timeout=20)
        structureResponse.raise_for_status()
        structureXML = ET.fromstring(structureResponse.content)
        namespaces = {
            'message': "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
            'structure': "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            'common': "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common"
        }

        dimension_ids = []
        codelist_ids = []

        for dimension in structureXML.findall('.//structure:Dimension', namespaces):
            dim_id = dimension.get('id')
            enumeration = dimension.find('./structure:LocalRepresentation/structure:Enumeration/Ref', namespaces)
            
            if enumeration is not None:
                codelist_id = enumeration.get('id')
                dimension_ids.append(dim_id)
                codelist_ids.append(codelist_id)

        structure = pd.DataFrame({
            'columns': dimension_ids,
            'codeList': codelist_ids
        })
          
        return structure
    except Exception as e:
        print(f"Error retrieving data structure: {e}")
        return pd.DataFrame(columns=['columns', 'codeList'])


if __name__ == '__main__':
    cd = get_dataStructure("CL_ITTER107")
    print(cd)