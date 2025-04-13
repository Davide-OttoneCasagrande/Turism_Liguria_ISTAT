CREATE OR REPLACE VIEW dim_accommodation_category AS
SELECT DISTINCT dim."index" AS id, dim.name as accommodation_category
FROM "CL_TIPO_ALLOGGIO2" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."TYPE_ACCOMMODATION";