-- stg_rxnorm__all_ndcs.sql

select
   {{ ndc_to_11 ('rxnsat.atv') }}as ndc11
    , rxnsat.atv as ndc
    , rxnsat.rxcui
    , rxnsat.sab
	, case when rxnsat.suppress = 'N' then true else false end as active
	, case when rxnsat.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnsat rxnsat
    where rxnsat.atn = 'NDC'
	and rxnsat.sab in ('ATC', 'CVX', 'DRUGBANK', 'MSH', 'MTHCMSFRF', 'MTHSPL', 'RXNORM', 'USP', 'VANDF')
