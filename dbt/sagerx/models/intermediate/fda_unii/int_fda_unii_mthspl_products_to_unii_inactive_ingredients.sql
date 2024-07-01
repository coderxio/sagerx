-- int_fda_unii_mthspl_products_to_unii_inactive_ingredients.sql

with products_to_inactive_ingredients as (
    select * from {{ ref('int_mthspl_products_to_inactive_ingredients') }}
)

, unii_codes as (
    select * from {{ ref('stg_fda_unii__unii_codes') }}
)


select
    ndc9
    , ndc
    , product_rxcui
    , product_name
    , product_tty
    , inactive_ingredient_unii
    , unii_codes.display_name as inactive_ingredient_unii_display_name
    , inactive_ingredient_rxcui
    , inactive_ingredient_name
    , inactive_ingredient_tty
    , active
    , prescribable
from products_to_inactive_ingredients
/*
need to join unii_codes twice - once
to pull in the actual UNII -> display name
mapping, and another initial one to try
to map substance RXCUIs to FDA UNII RXCUIs.
*/


left join unii_codes rxcui_to_unii
    on rxcui_to_unii.rxcui = inactive_ingredient_rxcui


/*
if MTHSPL (DailyMed) has a substance UNII,
use that. if it does not, try to map the
substance RXCUI to the FDA UNII RXCUI and
then use the resulting matched UNII to pull
in the UNII display name.
*/
left join unii_codes
    --on unii_codes.unii = inactive_ingredient_unii


    on unii_codes.unii = case
        when (
            inactive_ingredient_unii is not null 
            and
            inactive_ingredient_unii != 'NOCODE'
        ) then inactive_ingredient_unii
        else rxcui_to_unii.unii
        end

