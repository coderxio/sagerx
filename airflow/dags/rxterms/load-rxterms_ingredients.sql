/* datasource.rxterms_ingredients */
DROP TABLE IF EXISTS datasource.rxterms_ingredients;

CREATE TABLE datasource.rxterms_ingredients (
rxcui           TEXT,
ingredient      TEXT,
ing_rxcui       TEXT
);

COPY datasource.rxterms_ingredients
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxterms') }}/RxTermsIngredients{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m' ) }}.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
