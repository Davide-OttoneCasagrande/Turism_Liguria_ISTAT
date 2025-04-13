CREATE OR REPLACE VIEW dim_geographic_entity AS
SELECT DISTINCT dim."index" AS id, dim.name as geographic_entity
FROM "CL_ISO" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."COUNTRY_RES_GUESTS";