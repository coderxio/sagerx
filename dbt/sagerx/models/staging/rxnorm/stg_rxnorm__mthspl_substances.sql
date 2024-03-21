-- stg_rxnorm__mthspl_substances.sql

select
	substance.rxcui rxcui
	, substance.str name
	, substance.tty tty
	, substance.rxaui rxaui
	, substance.code unii
	, case when substance.suppress = 'N' then true else false end as active
	, case when substance.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso substance
where substance.tty = 'SU'
	and substance.sab = 'MTHSPL'
