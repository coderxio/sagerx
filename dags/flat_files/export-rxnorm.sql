COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_clinical_product_component) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_clinical_product_component.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_dose_form) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_dose_form.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_ingredient) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_ingredient.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_ingredient_component) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_ingredient_component.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_ingredient_strength) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_ingredient_strength.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.rxnorm_clinical_product_to_ndc) TO '/opt/airflow/extracts/rxnorm_clinical_product_to_ndc.txt' CSV HEADER DELIMITER '|';
