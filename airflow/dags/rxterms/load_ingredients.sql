/* sagerx_lake.rxterms_ingredients */
DROP TABLE IF EXISTS sagerx_lake.rxterms_ingredients;

CREATE TABLE sagerx_lake.rxterms_ingredients (
rxcui           TEXT,
ingredient      TEXT,
ing_rxcui       TEXT
);

COPY sagerx_lake.rxterms_ingredients
FROM '{data_path}/RxTermsIngredients{mnth}.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
