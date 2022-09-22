/* flatfile.nadac*/
CREATE OR REPLACE VIEW flatfile.nadac
AS 
   SELECT ndc
         ,ndc_description
         ,price_line
   		,nadac_per_unit
   		,pricing_unit 
   		,price_start_date
         ,CASE WHEN price_line = 1 THEN 'Y' END AS current_flag
         ,CASE WHEN price_line = FIRST_VALUE(price_line) OVER (PARTITION BY ndc ORDER BY price_line DESC) THEN 'Y' END AS first_price
         ,(nadac_per_unit - LAG(nadac_per_unit) OVER (PARTITION BY ndc ORDER BY price_line DESC)) AS dollar_change
         ,(nadac_per_unit - LAG(nadac_per_unit) OVER (PARTITION BY ndc ORDER BY price_line DESC))/
         					LAG(nadac_per_unit) OVER (PARTITION BY ndc ORDER BY price_line DESC) AS percent_change
         ,CASE WHEN (nadac_per_unit - LAG(NADAC_Per_Unit) OVER (PARTITION BY ndc ORDER BY price_line DESC)) > 0 THEN 1
         	   WHEN (nadac_per_unit - LAG(NADAC_Per_Unit) OVER (PARTITION BY ndc ORDER BY price_line DESC)) = 0 THEN 0
         	   WHEN (nadac_per_Unit - LAG(NADAC_Per_Unit) OVER (PARTITION BY ndc ORDER BY price_line DESC)) IS NULL THEN NULL
         	   ELSE -1 END AS change_type
		
   FROM (SELECT ndc
            ,ndc_description
            ,ROW_NUMBER() OVER (Partition By ndc ORDER BY effective_date DESC) AS price_line
            ,effective_Date AS price_start_date
            ,LAG(effective_date, 1) OVER (PARTITION BY ndc ORDER BY effective_date DESC) price_end_date
            ,nadac_per_unit
            ,pricing_unit
         FROM staging.nadac) nadac;