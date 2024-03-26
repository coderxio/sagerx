-- stg_rxnorm_historical__ndcs.sql

select
    *
from {{ source('rxnorm_historical', 'rxnorm_historical') }}
