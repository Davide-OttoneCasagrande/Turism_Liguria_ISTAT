SELECT distinct dm.id, dm.nome
FROM dim_cl_itter107 dm
INNER JOIN facts_turismo ft ON dm.id = ft."REF_AREA"