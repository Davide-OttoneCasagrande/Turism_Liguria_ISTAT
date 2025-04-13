CREATE OR REPLACE VIEW dim_gerarchia_luogo AS
SELECT  dim."index" AS id, dim2."index" as parent_ID, dim."nome" as nome
FROM "CL_LOCATION_HIERARCHY" dim
LEFT JOIN "CL_LOCATION_HIERARCHY" dim2
    ON dim.parent_id = dim2.id;