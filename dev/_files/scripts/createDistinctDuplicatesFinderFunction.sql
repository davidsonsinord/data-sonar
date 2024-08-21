CREATE OR REPLACE FUNCTION find_distinct_duplicates(p_schema_name text, p_table_name text)
RETURNS TABLE(duplicate_row text) AS $$
DECLARE
    columns text;
    formatted_columns text;
    query text;
    column_list text;
BEGIN
    -- Retrieve all columns from the specified schema's table, quoting identifiers
    SELECT string_agg(format('%I', column_name), ', ') INTO columns
    FROM information_schema.columns
    WHERE table_name = p_table_name
    AND table_schema = p_schema_name;

    -- Ensure there are columns
    IF columns IS NULL THEN
        RAISE EXCEPTION 'Table % does not exist or has no columns in schema %', p_table_name, p_schema_name;
    END IF;

    -- Build the column list with aliases to avoid ambiguities, quoting identifiers
    SELECT string_agg(format('t.%I = d.%I', column_name, column_name), ' AND ') INTO column_list
    FROM information_schema.columns
    WHERE table_name = p_table_name
    AND table_schema = p_schema_name;

    -- Build the formatted column list with quote_literal to wrap each value in single quotes, quoting identifiers
    SELECT string_agg(format('quote_literal(t.%I)', column_name), ' || '','' || ') INTO formatted_columns
    FROM information_schema.columns
    WHERE table_name = p_table_name
    AND table_schema = p_schema_name;

    -- Construct the query to find duplicates and return only the first occurrence of each duplicate set
    query := format('
        WITH duplicates AS (
            SELECT %s, COUNT(*) AS count
            FROM %I.%I
            GROUP BY %s
            HAVING COUNT(*) > 1
        ),
        first_occurrences AS (
            SELECT DISTINCT ON (%s) t.*, ''('' || %s || '')'' AS duplicate_row
            FROM %I.%I t
            JOIN duplicates d
            ON %s
            ORDER BY %s
        )
        SELECT duplicate_row
        FROM first_occurrences',
        columns, p_schema_name, p_table_name, columns, 
        columns, formatted_columns, p_schema_name, p_table_name, column_list, columns
    );

    -- Execute the dynamic query
    RETURN QUERY EXECUTE query;
END;
$$ LANGUAGE plpgsql;