/* datasource.rxnrel */
DROP TABLE IF EXISTS datasource.rxnrel;

CREATE TABLE datasource.rxnrel (
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
    blank     varchar(1)
);

COPY datasource.rxnrel
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm') }}/rrf/RXNREL.RRF' with (delimiter '|', null '');