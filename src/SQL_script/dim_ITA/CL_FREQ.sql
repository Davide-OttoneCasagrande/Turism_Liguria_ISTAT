CREATE OR REPLACE VIEW dim_frequenza AS
SELECT DISTINCT dim."index" AS id, dim.nome as frequenza
FROM "CL_FREQ" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."FREQ";