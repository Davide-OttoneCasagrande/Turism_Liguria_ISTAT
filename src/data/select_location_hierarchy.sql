SELECT distinct dm.id, dm.nome, dm.index
FROM "CL_ITTER107"  dm
INNER JOIN facts_turism ft ON dm.id = ft."REF_AREA"