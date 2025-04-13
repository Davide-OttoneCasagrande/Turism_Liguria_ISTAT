# REST request ISTAT API data
dataType = 0  # 0 = CSV  1 = JSON
max_num_filter = 18
timeframe = 'startPeriod=2016-01-01'
searchId:str = "ITC3" #codice per la liguria
originFilterSTR:str = "CL_ITTER107"
regionId_Length:int = 4
provinceId_length:int = 5
communeId_lenght:int = 6
dataflows = [
    ('122_54',"DCSC_TUR","facts_turism_raw"),
    ("161_268","DCSP_SBSREG","facts_indicatori_economici_raw")
]
facts_FolderPath = "src\\SQL_script"
dim_FolderPath = "src\\SQL_script\\dim_ITA"
