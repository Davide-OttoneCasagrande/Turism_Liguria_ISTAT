CREATE OR REPLACE VIEW dim_type_data_turism AS
SELECT DISTINCT dim."index" AS id, dim.name as type_data_turism
FROM "CL_TIPO_DATO7" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."DATA_TYPE";