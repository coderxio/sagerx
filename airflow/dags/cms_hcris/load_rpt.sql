/* sagerx_lake.cms_hcris_rpt */
DROP TABLE IF EXISTS sagerx_lake.cms_hcris_rpt CASCADE;

CREATE TABLE sagerx_lake.cms_hcris_rpt (
    RPT_REC_NUM         NUMERIC,
    PRVDR_CTRL_TYPE_CD  CHAR(2),
    PRVDR_NUM           CHAR(6),
    NPI                 NUMERIC,
    RPT_STUS_CD         CHAR(1),
    FY_BGN_DT           DATE,
    FY_END_DT           DATE,
    PROC_DT             DATE,
    INITL_RPT_SW        CHAR(1),
    LAST_RPT_SW         CHAR(1),
    TRNSMTL_NUM         CHAR(2),
    FI_NUM              CHAR(5),
    ADR_VNDR_CD         CHAR(1),
    FI_CREAT_DT         DATE,
    UTIL_CD             CHAR(1),
    NPR_DT              DATE,
    SPEC_IND            CHAR(1),
    FI_RCPT_DT          DATE,
    PRIMARY KEY (RPT_REC_NUM)
);

COPY sagerx_lake.cms_hcris_rpt
FROM '{data_path}/HOSP10_2024_rpt.csv' DELIMITER ',' CSV HEADER;;

CREATE INDEX IF NOT EXISTS x_cms_hcris_rpt_rpt_rec_num
ON sagerx_lake.cms_hcris_rpt(RPT_REC_NUM);

CREATE INDEX IF NOT EXISTS x_cms_hcris_rpt_prvdr_num
ON sagerx_lake.cms_hcris_rpt(PRVDR_NUM);

CREATE INDEX IF NOT EXISTS x_cms_hcris_rpt_npi
ON sagerx_lake.cms_hcris_rpt(NPI);

