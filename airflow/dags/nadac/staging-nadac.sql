/* staging.nadac */
create table if not exists staging.nadac (
	ndc 				varchar(11) not null,
	ndc_description		text not null,
   	nadac_per_unit 		numeric,
	pricing_unit 		text,
	effective_date		date,
	primary key (ndc, effective_date)
); 

insert into staging.nadac
select distinct 
	n.ndc
	,ndc_description
	,n.nadac_per_unit::numeric
	,n.pricing_unit
	,n.effective_date::date

from datasource.nadac n
on conflict do nothing
;