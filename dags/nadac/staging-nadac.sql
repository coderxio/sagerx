 /* staging.nadac */
 DROP TABLE IF EXISTS staging.nadac CASCADE;

 CREATE TABLE staging.nadac (
	ndc 				varchar(11) NOT NULL,
	ndc_description		TEXT NOT NULL,
	price_line 			int NOT NULL,
	price_start_date 	date,
	price_end_date 		date,
   	nadac_per_unit 		numeric,
	pricing_Unit 		TEXT,
	PRIMARY KEY (NDC, Price_Line)
); 

INSERT INTO staging.nadac
SELECT ndc
	,ndc_description
	,ROW_NUMBER() OVER (Partition By ndc ORDER BY effective_date DESC) AS price_line
	,effective_Date AS price_start_date
	,LAG(effective_date, 1) OVER (PARTITION BY ndc ORDER BY effective_date DESC) price_end_date
	,nadac_per_unit
	,pricing_unit
	
FROM (Select DISTINCT 
		n.ndc
		,ndc_description
		,n.nadac_per_unit::numeric
		,n.pricing_unit
		,n.effective_date::date

	  FROM datasource.nadac n) z;