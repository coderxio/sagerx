-- gtins.sql

with

ndcs as (

    select
        ndc11,
        replace(ndcpackagecode, '-', '') as ndc10,
        ndcpackagecode as ndc,
        concat(
            '003',
            replace(ndcpackagecode,'-', '')
        ) as gtin13,
        concat(
            '3',
            replace(ndcpackagecode,'-', '')
        ) as gtin11,
        concat(
            '03',
            split_part(ndcpackagecode, '-', 1)
        ) as gs1_company_prefix

    from {{ ref('stg_fda_ndc__ndcs') }}

),

digits as (

    -- split the 13-digit number into individual digits
    select 
        ndc,
        position,
        substring(gtin13 from position for 1)::int as digit
    from ndcs,
        generate_series(1, 13) as position

),

products as (

    -- apply the alternating multiplication rule
    select 
        *,
        case
            when position % 2 = 1
                then digit * 3
            else digit * 1 
        end as product
    from digits

),

sums as (

    -- sum of the products of each digit
    select
        ndc,
        sum(product) as sum
    from products
    group by ndc

),

check_digits as (

    -- round the sum to the nearest 10 and subtract the sum
    select
        ndc,
        ceil(sum / 10.0) * 10 - sum as check_digit
    from sums

),

gtin14s as (

    -- concatenate the gtin13 and check_digit
    select
        ndc11,
        ndc10,
        ndcs.ndc,
        concat(
            gtin13,
            check_digit
        ) as gtin14,
        concat(
            gtin11,
            check_digit
        ) as gtin12,
        gs1_company_prefix
    from ndcs
    left join check_digits
        on check_digits.ndc = ndcs.ndc

)

select * from gtin14s
