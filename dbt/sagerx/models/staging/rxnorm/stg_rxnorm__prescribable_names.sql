-- stg_rxnorm__prescribable_names.sql

select
    rxcui,
    str as prescribable_name
from {{ source('rxnorm', 'rxnorm_rxnconso') }}
where sab = 'RXNORM'
    and tty = 'PSN'
