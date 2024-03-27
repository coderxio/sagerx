with all_ndcs as
(
    select * from {{ ref('stg_rxnorm__all_ndcs') }}
),

product_rxcuis as
(
    select * from sagerx_lake.rxnorm_rxnconso
    where sab = 'RXNORM'
        and tty in ('SCD', 'SBD', 'GPCK', 'BPCK')
)

select distinct
    all_ndcs.ndc11
    , product_rxcuis.rxcui
from all_ndcs
inner join product_rxcuis
    on all_ndcs.rxcui = product_rxcuis.rxcui
