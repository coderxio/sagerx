-- stg_rxnorm__clinical_products.sql
with

clinical_products as (

    select
        rxcui,
        str as name,
        tty,
        {{ active_and_prescribable() }}

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}

    where
        tty in (
            'SCD',
            'GPCK'
        ) and
        sab = 'RXNORM'

)

select * from clinical_products
