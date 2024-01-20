/* datasource.rxterms_ingredients */
DROP TABLE IF EXISTS datasource.rxterms_ingredients;

CREATE TABLE datasource.rxterms_ingredients (
rxcui           TEXT,
ingredient      TEXT,
ing_rxcui       TEXT
);

COPY datasource.rxterms_ingredients
FROM '{data_path}/RxTermsIngredients{mnth}.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
