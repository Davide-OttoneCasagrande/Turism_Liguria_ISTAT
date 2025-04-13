CREATE OR REPLACE VIEW dim_caratteristiche_alloggio AS
SELECT DISTINCT dim."index" AS id, dim.nome as categoria_alloggio
FROM "CL_TIPO_ALLOGGIO2" dim
RIGHT JOIN facts_turism_raw ft
	ON dim.id = ft."TYPE_ACCOMMODATION";