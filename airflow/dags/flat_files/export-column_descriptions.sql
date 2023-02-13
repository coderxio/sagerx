/* flatfile.column_descriptions */
CREATE OR REPLACE VIEW flatfile.column_descriptions
AS 
    SELECT
        c.table_schema
        , c.table_name
        , c.column_name
        , pgd.description
    FROM pg_catalog.pg_statio_all_tables AS st
        INNER JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid
        INNER JOIN information_schema.columns c 
            ON pgd.objsubid = c.ordinal_position
            AND  c.table_schema=st.schemaname
            AND c.table_name=st.relname;

   COPY (SELECT * FROM flatfile.column_descriptions) TO '/opt/airflow/extracts/column_descriptions.txt' CSV HEADER DELIMITER '|'
