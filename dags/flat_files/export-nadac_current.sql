CREATE OR REPLACE VIEW flatfile.nadac_current
AS 
SELECT *
FROM flatfile.nadac nadac
   WHERE nadac.price_line = 1;

COPY (SELECT * FROM flatfile.nadac_current) TO '/opt/airflow/extracts/nadac_current.txt' CSV HEADER DELIMITER '|'
