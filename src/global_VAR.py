# REST request ISTAT API data
language_is_ITA: bool = True
max_num_filter:int = 18
timeframe:str = 'startPeriod=2016-01-01'
searchId:str = "ITC3" #codice per la liguria
originFilterSTR:str = "CL_ITTER107"
ID_lengths = {
    "region": 4,
    "province": 5,
    "commune": 6
}
dataflows = [
    ('122_54',"DCSC_TUR","facts_turism_raw"),
    ("161_268","DCSP_SBSREG","facts_indicatori_economici_raw")
]
#folders pathS
drop_script_path: dict[str, str] = {"root":"src", "path":"SQL_script", "name":"drop_all_views.sql"}
facts_FolderPath: dict[str, str] = {"root":"src", "path":"SQL_script", "name":"facts"}
if language_is_ITA:
    dim_FolderPath: dict[str, str] = {"root":"src", "path":"SQL_script", "name":"dim_ITA"}
else:
    dim_FolderPath: dict[str, str] = {"root":"src", "path":"SQL_script", "name":"dim_ENG"}