/* sagerx_lake.cms_hcris_alpha */
DROP TABLE IF EXISTS sagerx_lake.cms_hcris_alpha CASCADE;

CREATE TABLE sagerx_lake.cms_hcris_alpha (
    RPT_REC_NUM             NUMERIC,
    WKSHT_CD                CHAR(7),
    LINE_NUM                CHAR(5),
    CLMN_NUM                CHAR(5),
    ITM_ALPHNMRC_ITM_TXT    CHAR(40)
);

COPY sagerx_lake.cms_hcris_alpha
FROM '{data_path}/HOSP10_2024_alpha.csv' DELIMITER ',' CSV HEADER;;

CREATE INDEX IF NOT EXISTS x_cms_hcris_alpha_rpt_rec_num
ON sagerx_lake.cms_hcris_alpha(RPT_REC_NUM);

CREATE INDEX IF NOT EXISTS x_cms_hcris_alpha_wksht_cd
ON sagerx_lake.cms_hcris_alpha(WKSHT_CD);

