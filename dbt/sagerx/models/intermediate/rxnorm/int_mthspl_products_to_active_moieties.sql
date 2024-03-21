-- int_mthspl_products_to_active_moieties.sql

with

substance as (

    select * from {{ ref('stg_rxnorm__mthspl_substances') }}

),

product as (

    select * from {{ ref('stg_rxnorm__mthspl_products') }}

)

select distinct
    concat(lpad(split_part(product.ndc,'-', 1), 5, '0'), lpad(split_part(product.ndc,'-', 2), 4, '0')) as ndc9
    , product.ndc as ndc
    , product.rxcui as product_rxcui
    , product.name as product_name
    , product.tty as product_tty
    , substance.unii as active_moiety_unii
    , substance.rxcui as active_moiety_rxcui
    , substance.name as active_moiety_name
    , substance.tty as active_moiety_tty	
    , product.active as active
    , product.prescribable as prescribable
from sagerx_lake.rxnorm_rxnrel rxnrel
inner join substance
    on rxnrel.rxaui1 = substance.rxaui
inner join product
    on rxnrel.rxaui2 = product.rxaui
where rela = 'has_active_moiety'
