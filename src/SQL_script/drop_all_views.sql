DO $$
DECLARE
    vw RECORD; -- Variable to loop through views
BEGIN
    -- Loop through each view in the current schema
    FOR vw IN
        SELECT table_schema || '.' || table_name AS full_view_name
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
    LOOP
        EXECUTE 'DROP VIEW IF EXISTS ' || vw.full_view_name || ' CASCADE;';
    END LOOP;
END;
$$;