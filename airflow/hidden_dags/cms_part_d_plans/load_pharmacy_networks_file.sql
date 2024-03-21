/* sagerx_lake.cms_pharmacy_networks */
DROP TABLE IF EXISTS sagerx_lake.cms_pharmacy_networks CASCADE;

CREATE TABLE sagerx_lake.cms_pharmacy_networks (
contract_id           VARCHAR(5) NOT NULL,
plan_id         VARCHAR(3) NOT NULL,
segment_id      VARCHAR(3),
pharmacy_number  VARCHAR(12),
pharmacy_zipcode  VARCHAR(5),
preferred_status_retail    VARCHAR(1),
preferred_status_mail    VARCHAR(1),
pharmacy_retail      VARCHAR(1),
pharmacy_mail      VARCHAR(1),
in_area_flag      TEXT,
floor_price      TEXT,
brand_dispensing_fee_30       TEXT,
brand_dispensing_fee_60       TEXT,
brand_dispensing_fee_90       TEXT,
generic_dispensing_fee_30       TEXT,
generic_dispensing_fee_60       TEXT,
generic_dispensing_fee_90       TEXT
);

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 1.txt' DELIMITER '|' CSV HEADER;;

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 2.txt' DELIMITER '|' CSV HEADER;;

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 3.txt' DELIMITER '|' CSV HEADER;;

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 4.txt' DELIMITER '|' CSV HEADER;;

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 5.txt' DELIMITER '|' CSV HEADER;;

COPY sagerx_lake.cms_pharmacy_networks
FROM '{{ ti.xcom_pull(task_ids='extract') }}/pharmacy networks file  PPUF_{{params.year}}Q{{params.quarter}} part 6.txt' DELIMITER '|' CSV HEADER;;
