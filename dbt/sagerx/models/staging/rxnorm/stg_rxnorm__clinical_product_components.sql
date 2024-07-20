-- stg_rxnorm__clinical_product_components.sql
with

clinical_product_components as (

    select
        rxcui,
        str as name,
        tty

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}

        where sab = 'RXNORM' and
            tty = 'SCD' 

),

rxnrel as (

    select
        *

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }}

    where rela in (
        'consists_of',
        'has_ingredients',
        'has_ingredient'
    )

),

ingredients AS (

    select
        rxcui,
        str as name,
        tty

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}

    where sab = 'RXNORM' and
        tty in (
            'IN',
            'MIN' 
        )

),

cpc_to_multiple_ingredients as (

    select
        cpc.rxcui as clinical_product_component_rxcui,
        rxnrel.rxcui1 as ingredient_rxcui,

    from
        clinical_product_components cpc
        
    inner join rxnrel
        on rxnrel.rxcui2 = cpc.rxcui and
            rxnrel.rela = 'has_ingredients'

),

cpc_to_single_ingredients as (

    select
        cpc.rxcui
            as clinical_product_component_rxcui,
        ingredient_rxnrel.rxcui1 as ingredient_rxcui,

    from
        clinical_product_components cpc
        
    inner join rxnrel as scdc_rxnrel
        on scdc_rxnrel.rxcui2 = cpc.rxcui and
            scdc_rxnrel.rela = 'consists_of'
    inner join rxnrel as ingredient_rxnrel
        on ingredient_rxnrel.rxcui2 = scdc_rxnrel.rxcui1 and
            ingredient_rxnrel.rela = 'has_ingredient'

),

cpc_to_all_ingredients as (

    select
        *
    from clinical_products_to_multiple_ingredients

    union all

    select
        *
    from clinical_products_to_single_ingredients

)


ranked_clinical_product_component_ingredients as (

    select
        *,
        row_number() over(
            partition by clinical_product_component_rxcui
            order by
                ingredient_tty desc
        ) as rn
    from cpc_to_all_ingredients

)
select
    rxcui,
    name,
    tty,
    {{ active_and_prescribable() }},
    cte.ingredient_rxcui AS ingredient_rxcui,
    dose_form_rxnrel.rxcui1 as dose_form_rxcui
from
    clinical_product_components
    /*
    left join ingredients
        on ingredients.clinical_product_component_rxcui = 
            clinical_product_components.rxcui
    left join dose_forms
        on dose_forms.clinical_product_component_rxcui = 
            clinical_product_components.rxcui
    */
    left join cte
    on cte.product_component_rxcui = case
        when product_component.rxcui is null then product.rxcui
        else product_component.rxcui
    end
    and cte.rn < 2
    left join rxnrel as dose_form_rxnrel
    on dose_form_rxnrel.rxcui2 = case
        when product_component.rxcui is null then product.rxcui
        else product_component.rxcui
    end
    and dose_form_rxnrel.rela = 'has_dose_form'
    and dose_form_rxnrel.sab = 'RXNORM'
