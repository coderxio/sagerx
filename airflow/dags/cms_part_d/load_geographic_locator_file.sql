/* sagerx_lake.cms_geographic_locator */
DROP TABLE IF EXISTS sagerx_lake.cms_geographic_locator CASCADE;

CREATE TABLE sagerx_lake.cms_geographic_locator (
county_code           VARCHAR(5) NOT NULL,
statename         VARCHAR(30) NOT NULL,
county      VARCHAR(50),
ma_region_code  VARCHAR(2),
ma_region  VARCHAR(150),
pdp_region_code  VARCHAR(2),
pdp_region    VARCHAR(150)
);

COPY sagerx_lake.cms_geographic_locator
FROM '{{ ti.xcom_pull(task_ids='extract') }}/geographic locator file PPUF_{{params.year}}Q{{params.quarter}}.txt' DELIMITER '|' CSV HEADER;;
