CREATE OR REPLACE VIEW facts_turism AS
SELECT
cf."index" as "FREQ",
clh."index" as "REF_AREA",
ctd."index"as "DATA_TYPE",
cta."index" as "TYPE_ACCOMMODATION",
ci."index" as "COUNTRY_RES_GUESTS",
ft."TIME_PERIOD",
ft."OBS_VALUE"
FROM facts_turism_raw ft 
LEFT JOIN "CL_FREQ" cf
    ON ft."FREQ" = cf.id
LEFT JOIN "CL_LOCATION_HIERARCHY" clh
    ON ft."REF_AREA" = clh.id
LEFT JOIN "CL_TIPO_DATO7" ctd
    ON ft."DATA_TYPE" = ctd.id
LEFT JOIN "CL_TIPO_ALLOGGIO2" cta
    ON ft."TYPE_ACCOMMODATION" = cta.id
LEFT JOIN "CL_ISO" ci
    ON ft."COUNTRY_RES_GUESTS" = ci.id;