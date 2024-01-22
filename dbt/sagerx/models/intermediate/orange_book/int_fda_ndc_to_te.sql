-- DISCLAIMER: This model is under development and incomplete.

with cte as (
    select
        fda.ndc11
        , obp.te_code
        , count(fda.ndc11) over( partition by fda.ndc11 ) as num_te_codes
    from {{ source('orange_book', 'orange_book_products') }} as obp
    inner join {{ ref('stg_fda_ndc__ndcs') }} as fda 
        on concat(case when obp.appl_type = 'A' then 'ANDA' else 'NDA' end, obp.appl_no) = fda.applicationnumber
    group by fda.ndc11, obp.te_code
)
select
    fda.ndc11
    , fda.applicationnumber as application_number
    , cte.te_code
    , left(cte.te_code, 2) as first_two_te_code
    , left(cte.te_code, 1) as first_one_te_code
from {{ ref('stg_fda_ndc__ndcs') }} as fda 
inner join cte 
    on fda.ndc11 = cte.ndc11 
    and cte.num_te_codes = 1
