select distinct 
    b.rxcui, 
    a.atv as hcpcs_code, 
    b.tty, 
    b.str as drug_name
from sagerx_lake.rxnorm_rxnsat a
join sagerx_lake.rxnorm_rxnconso b on a.rxcui = b.rxcui
where a.atn = 'DHJC'
and a.atv like 'J%'
and b.tty in ('GPCK', 'BPCK', 'SCD', 'SBD')
order by a.atv
