CREATE OR REPLACE VIEW dim_entita_geografica AS
SELECT DISTINCT dim."index" AS id, dim.nome as entita_geografica
FROM "CL_ISO" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."COUNTRY_RES_GUESTS";