/* sagerx_lake.cms_hcris_nmrc */
DROP TABLE IF EXISTS sagerx_lake.cms_hcris_nmrc CASCADE;

CREATE TABLE sagerx_lake.cms_hcris_nmrc (
    RPT_REC_NUM     NUMERIC,
    WKSHT_CD        CHAR(7),
    LINE_NUM        CHAR(5),
    CLMN_NUM        CHAR(5),
    ITM_VAL_NUM     NUMERIC
);

COPY sagerx_lake.cms_hcris_nmrc
FROM '{data_path}/HOSP10_2024_nmrc.csv' DELIMITER ',' CSV HEADER;;

CREATE INDEX IF NOT EXISTS x_cms_hcris_nmrc_rpt_rec_num
ON sagerx_lake.cms_hcris_nmrc(RPT_REC_NUM);

CREATE INDEX IF NOT EXISTS x_cms_hcris_nmrc_wksht_cd
ON sagerx_lake.cms_hcris_nmrc(WKSHT_CD);

