CREATE OR REPLACE VIEW dim_codice_ateco AS
SELECT DISTINCT dim."index" AS id, dim.nome as codice_ateco
FROM "CL_ATECO_2007" dim
RIGHT JOIN facts_indicatori_economici_raw ft
	ON dim.id = ft."ECON_ACTIVITY_NACE_2007";