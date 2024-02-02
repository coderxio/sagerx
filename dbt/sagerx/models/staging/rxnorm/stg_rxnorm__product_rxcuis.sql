-- stg_rxnorm__product_rxcuis

select * from {{ source('rxnorm', 'rxnorm_rxnconso') }}
where sab = 'RXNORM'
    and tty in ('SCD', 'SBD', 'GPCK', 'BPCK')
