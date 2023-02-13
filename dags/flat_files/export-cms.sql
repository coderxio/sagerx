COPY (SELECT * FROM flatfile.cms_all) TO '/opt/airflow/extracts/cms_all.txt' CSV HEADER DELIMITER '|';
COPY (SELECT * FROM flatfile.opps_asp_mapping) TO '/opt/airflow/extracts/opps_asp_mapping.txt' CSV HEADER DELIMITER '|';
