CREATE OR REPLACE VIEW dim_frequency AS
SELECT DISTINCT dim."index" AS id, dim.name as frequency
FROM "CL_FREQ" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."FREQ";