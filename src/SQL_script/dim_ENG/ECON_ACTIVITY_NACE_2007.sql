CREATE OR REPLACE VIEW dim_ateco_code AS
SELECT DISTINCT dim."index" AS id, dim.name as ateco_code
FROM "CL_ATECO_2007" dim
RIGHT JOIN facts_indicatori_economici_raw ft
	ON dim.id = ft."ECON_ACTIVITY_NACE_2007";