-- stg_fda__fda_ndc_class.sql

select
	z.productid
	,row_number() over (partition by z.productid order by z.token desc) as class_line
	,left(z.token, position('[' in z.token) -1 ) as class_name
    ,substring(z.token, '\[(.+)\]') as class_type
from (select distinct t.productid
	,t.pharm_classes
	, s.token
	from   datasource.fda_ndc_product t, unnest(string_to_array(t.pharm_classes, ',')) s(token)) z
