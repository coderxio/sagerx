-- int_mthspl_products_to_inactive_ingredients.sql

with

substance as (
    select * from {{ ref('stg_rxnorm__mthspl_substances') }}
)

, product as (
    select * from {{ ref('stg_rxnorm__mthspl_products') }}
)

, unii_codes as (
    select * from {{ ref('stg_fda_unii__unii_codes') }}
)

select distinct
    concat(lpad(split_part(product.ndc,'-', 1), 5, '0'), lpad(split_part(product.ndc,'-', 2), 4, '0')) as ndc9
    , product.ndc as ndc
    , product.rxcui as product_rxcui
    , product.name as product_name
    , product.tty as product_tty
    , substance.unii as inactive_ingredient_unii
    , unii_codes.display_name as inactive_ingredient_unii_display_name
    , substance.rxcui as inactive_ingredient_rxcui
    , substance.name as inactive_ingredient_name
    , substance.tty as inactive_ingredient_tty	
    , product.active as active
    , product.prescribable as prescribable
from sagerx_lake.rxnorm_rxnrel rxnrel
inner join substance
    on rxnrel.rxaui1 = substance.rxaui
inner join product
    on rxnrel.rxaui2 = product.rxaui
left join unii_codes rxcui_to_unii
    on rxcui_to_unii.rxcui = substance.rxcui
left join unii_codes
    on unii_codes.unii = case
        when (
            substance.unii is not null 
            and
            substance.unii != 'NOCODE'
        ) then substance.unii
        else rxcui_to_unii.unii
        end
where rela = 'has_inactive_ingredient'
