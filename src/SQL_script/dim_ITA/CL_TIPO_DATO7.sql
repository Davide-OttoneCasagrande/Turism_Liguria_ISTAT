CREATE OR REPLACE VIEW dim_tipo_dato_turismo AS
SELECT DISTINCT dim."index" AS id, dim.nome as tipo_dato_turismo
FROM "CL_TIPO_DATO7" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."DATA_TYPE";