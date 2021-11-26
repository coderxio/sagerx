DROP FOREIGN TABLE IF EXISTS staging.dag_run;
DROP FOREIGN TABLE IF EXISTS staging.task_instance;

IMPORT FOREIGN SCHEMA public LIMIT TO (dag_run,task_instance) FROM SERVER airflow_fdw INTO staging;

CREATE OR REPLACE VIEW flatfile.last_sql_task
AS 
SELECT * 
FROM (SELECT ti.task_id
		,SUBSTRING(ti.task_id FROM POSITION('-' IN ti.task_id) + 1  FOR (LENGTH(ti.task_id) - POSITION('-' IN ti.task_id) - 4))
		,ti.dag_id
		,ti.start_date
		,ti.end_date
		,ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY end_date DESC) AS ROW_NUM

	FROM staging.task_instance ti

	WHERE ti.state = 'success'
		AND POSITION('-' IN ti.task_id) > 0) z
	
WHERE ROW_NUM = 1;