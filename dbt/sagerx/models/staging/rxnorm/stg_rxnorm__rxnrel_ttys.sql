-- stg_rxnorm__rxnrel_ttys.sql

with

rxnrel as (

    select
        rxcui1,
        rxcui2,
        rela

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }} 

),

rxnconso as (

    select
        rxcui,
        tty

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }} 

    where sab = 'RXNORM'

)

select
    rxcui1,
    r1.tty as tty1,
    rxcui2,
    r2.tty as tty2,
    rela
from rxnrel
left join rxnconso r1
    on r1.rxcui = rxcui1
left join rxnconso r2
    on r2.rxcui = rxcui2
