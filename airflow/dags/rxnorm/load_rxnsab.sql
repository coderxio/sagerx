/* sagerx_lake.rxnorm_rxnsab */
DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxnsab CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxnsab  (
   vcui           varchar (8),
   rcui           varchar (8),
   vsab           varchar (40),
   rsab           varchar (20) NOT NULL,
   son            varchar (3000),
   sf             varchar (20),
   sver           varchar (20),
   vstart         varchar (10),
   vend           varchar (10),
   imeta          varchar (10),
   rmeta          varchar (10),
   slc            varchar (1000),
   scc            varchar (1000),
   srl            integer,
   tfr            integer,
   cfr            integer,
   cxty           varchar (50),
   ttyl           varchar (300),
   atnl           varchar (1000),
   lat            varchar (3),
   cenc           varchar (20),
   curver         varchar (1),
   sabin          varchar (1),
   ssn            varchar (3000),
   scit           varchar (4000),
   blank          TEXT
);

COPY sagerx_lake.rxnorm_rxnsab
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNSAB.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default