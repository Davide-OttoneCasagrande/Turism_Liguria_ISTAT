CREATE OR REPLACE VIEW dim_type_data_econimic_indicators AS
SELECT DISTINCT dim."index" AS id, dim.name as type_data_econimic_indicators
FROM "CL_TIPO_DATO29" dim
RIGHT JOIN facts_indicatori_economici_raw ft
	ON dim.id = ft."DATA_TYPE"::TEXT;