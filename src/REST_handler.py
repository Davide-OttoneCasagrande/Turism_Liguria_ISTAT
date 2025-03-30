import os
import requests as req
import pandas as pd
import io
import json
import ISTAT_API_Request_data as API_ISTAT
from dotenv import load_dotenv
load_dotenv()

dataTyPe = API_ISTAT.dataType
dataflows = API_ISTAT.dataflows
filters_122_54 = API_ISTAT.get_filter_for_dataflow(dataflow=dataflows[0][0])
filters_68_357 = API_ISTAT.get_filter_for_dataflow(dataflow=dataflows[1][0])
filters_161_268 = API_ISTAT.get_filter_for_dataflow(dataflow=dataflows[2][0])
timeframe = API_ISTAT.timeframe
headerCSV = {'Accept': 'text/csv'}
headerJSON = {'Accept': 'application/json'}

def get_dataflow(filters:list[str], dataflow:str, timeframe:str=timeframe) -> pd.DataFrame:
    """
    Fetch data from the ISTAT API.

    Args:
        filters (list[str]): The filter strings for the request.
        dataflow (str): The dataflow identifier.
        timeframe (str): The timeframe for the request.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched data.
    """
    dfs = []
    for filterID in filters:
        url =  f'https://esploradati.istat.it/SDMXWS/rest/data/{dataflow}/{filterID}?{timeframe}'

        try:
            response = req.get(url, headers=headerCSV, timeout=20)
            response.raise_for_status()
            df= pd.read_csv(io.StringIO(response.text), sep=';')
            dfs.append(df)
        except req.exceptions.HTTPError as e:
            if response.status_code == 404: # Log the 404 error
                print(f"Filter '{filterID}' returned a 404: Resource not found.")
            else:
                raise RuntimeError(f"Request failed for filter '{filterID}': {e}")
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        raise ValueError("No data returned from the API.")

def codelist(codeListName:str) -> pd.DataFrame:
    """
    Fetch the codelist from the ISTAT API.

    Args:
        dataflow (str): The dataflow identifier.

    Returns:
        pd.DataFrame: A DataFrame containing the codelist.
    """
    url = f'https://esploradati.istat.it/SDMXWS/rest/codelist/{codeListName}'
    try:
        response = req.get(url, headers=headerJSON, timeout=20)
        response.raise_for_status()
        return pd.json_normalize(response.json())
    except req.exceptions.RequestException as e:
        raise RuntimeError(f"Request failed for codelist: {e}")

def get_codelist(dataflowID:str) -> pd.DataFrame:
    urlDatastructure = f'https://esploradati.istat.it/SDMXWS/rest/datastructure/IT1/{dataflowID}'
    structureXML = req.get(urlDatastructure, headers=headerJSON, timeout=20)
    structureXML.raise_for_status()

    return pd.json_normalize(structureXML.json())

if __name__ == '__main__':
    #dataframe = get_dataflow(filters_122_54, dataflow=dataflows[0][0])
    cd = get_codelist(dataflows[0][1])

'''
    Accedere ai metadati

Questa è la struttura dell'URL per accedere ai metadati:

http://sdmx.istat.it/SDMXWS/rest/resource/agencyID/resourceID/version/itemID?queryStringParameters

Alcune note:

    resource (obbligatorio), la risorsa che si vuole interrogare (tra queste categorisation, categoryscheme, codelist, conceptscheme, contentconstraint, dataflow e datastructure);
    agencyID, l'identiticativo dell'agenzia che esponi i dati (qui è IT1);
    resourceID, l'ID della risorsa che si vuole interrogare (successivamente qualche esempio);
    version, la versione dell'artefatto che si vuole interrogare;
    itemID, l'ID dell'elemento (per schemi di elementi) o della gerarchia (per elenchi di codici gerarchici) da restituire;
    queryStringParameters
        detail, la quantità desiderata di informazioni. I valori possibili sono allstubs, referencestubs, allcompletestubs, referencecompletestubs, referencepartial e full e di default è full. di riferimento parziale, completo.
        references, riferimenti relativi da restituire. I valori possibili sono none, parents, parentsandsiblings, children, descendants, all, any e di default è none.
'''
