/* flatfile.nadac*/
TRUNCATE TABLE public.nadac;
INSERT INTO public.nadac
SELECT ndc || price_line::text AS ID
        ,ndc_description
        ,price_line
   		,nadac_per_unit
   		,pricing_unit 
   		,price_start_date
        ,current_flag
        ,first_price
        ,CASE WHEN dollar_change IS NULL THEN 0 ELSE dollar_change END AS dollar_change
        ,CASE WHEN percent_change IS NULL THEN 0 ELSE ROUND(percent_change * 100,2) END AS percent_change
        ,CASE WHEN fn.change_type = -1 THEN 'decrease'
              WHEN fn.change_type = 1 THEN 'increase'
              ELSE 'none' END AS change_type

FROM flatfile.nadac fn;