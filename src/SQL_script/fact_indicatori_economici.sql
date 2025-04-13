CREATE OR REPLACE VIEW facts_indicatori_economici AS
SELECT 
    ctd."index" AS "DATA_TYPE", 
    ca."index" AS "ECON_ACTIVITY_NACE_2007", 
    fie."TIME_PERIOD", 
    fie."OBS_VALUE"
FROM facts_indicatori_economici_raw fie
LEFT JOIN "CL_TIPO_DATO29" ctd 
    ON fie."DATA_TYPE"::TEXT = ctd.id
LEFT JOIN "CL_ATECO_2007" ca 
    ON fie."ECON_ACTIVITY_NACE_2007" = ca.id;