/* sagerx_lake.nadac */
CREATE TABLE IF NOT EXISTS sagerx_lake.nadac (
ndc_description		                            TEXT NOT NULL,
ndc			                                    VARCHAR (11) NOT NULL,
nadac_per_unit		                            NUMERIC (12,5),
effective_date                                  DATE NOT NULL,
pricing_unit		                            TEXT,
pharmacy_type_indicator	                        TEXT,
otc		                                        TEXT,
explanation_code		                        TEXT,
classification_for_rate_setting		            TEXT,
corresponding_generic_drug_nadac_per_unit		TEXT,
corresponding_generic_drug_effective_date		DATE,
as_of_date  			                        DATE
);

TRUNCATE sagerx_lake.nadac;

COPY sagerx_lake.nadac
FROM '{{ ti.xcom_pull(task_ids='extract') }}' CSV HEADER;