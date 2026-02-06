-- stg_rxnorm__available_strengths.sql

select
    atv as strength_name,
    *
from {{ source('rxnorm', 'rxnorm_rxnsat') }}
where sab = 'RXNORM'
    and atn = 'RXN_AVAILABLE_STRENGTH'
