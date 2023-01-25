/* staging.nadac */
CREATE TABLE IF NOT EXISTS staging.nadac (
	ndc 				varchar(11) NOT NULL,
	ndc_description		TEXT NOT NULL,
   	nadac_per_unit 		numeric,
	pricing_Unit 		TEXT,
	effective_date		DATE,
	PRIMARY KEY (NDC, effective_date)
); 

INSERT INTO staging.nadac
SELECT DISTINCT 
	n.ndc
	,ndc_description
	,n.nadac_per_unit::numeric
	,n.pricing_unit
	,n.effective_date::date

FROM datasource.nadac n
ON CONFLICT DO NOTHING
;