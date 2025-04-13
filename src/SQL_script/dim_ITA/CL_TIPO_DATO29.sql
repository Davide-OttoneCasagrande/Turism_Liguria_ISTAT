CREATE OR REPLACE VIEW dim_tipo_dato_indicatori_economici AS
SELECT DISTINCT dim."index" AS id, dim.nome as tipo_dato_indicatori_economici
FROM "CL_TIPO_DATO29" dim
RIGHT JOIN facts_indicatori_economici_raw ft
	ON dim.id = ft."DATA_TYPE"::TEXT;