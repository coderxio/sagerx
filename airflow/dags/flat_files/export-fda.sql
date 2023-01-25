COPY (SELECT * FROM staging.fda_ndc) TO '/opt/airflow/extracts/fda_ndc.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM staging.fda_ndc_class) TO '/opt/airflow/extracts/fda_ndc_class.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM staging.fda_ndc_substance) TO '/opt/airflow/extracts/fda_ndc_substance.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM staging.fda_excluded) TO '/opt/airflow/extracts/fda_excluded.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM staging.fda_unfinished) TO '/opt/airflow/extracts/fda_unfinished.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM staging.fda_unfinished_substance) TO '/opt/airflow/extracts/fda_unfinished_substance.txt' CSV HEADER DELIMITER '|';
