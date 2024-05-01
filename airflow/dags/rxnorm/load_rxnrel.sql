/* sagerx_lake.rxnorm_rxnrel */
DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxnrel CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxnrel (
    rxcui1    varchar(8) ,
    rxaui1    varchar(8),
    stype1    varchar(50),
    rel       varchar(4) ,
    rxcui2    varchar(8) ,
    rxaui2    varchar(8),
    stype2    varchar(50),
    rela      varchar(100) ,
    rui       varchar(10),
    srui      varchar(50),
    sab       varchar(20) NOT NULL,
    sl        varchar(1000),
    dir       varchar(1),
    rg        varchar(10),
    suppress  varchar(1),
    cvf       varchar(50),
    blank     TEXT
);

COPY sagerx_lake.rxnorm_rxnrel
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNREL.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default

CREATE INDEX IF NOT EXISTS rxnrel_rxcui1
ON sagerx_lake.rxnorm_rxnrel(rxcui1);

CREATE INDEX IF NOT EXISTS rxnrel_rxcui2
ON sagerx_lake.rxnorm_rxnrel(rxcui2);

CREATE INDEX IF NOT EXISTS rxnrel_rela
ON sagerx_lake.rxnorm_rxnrel(rela);
