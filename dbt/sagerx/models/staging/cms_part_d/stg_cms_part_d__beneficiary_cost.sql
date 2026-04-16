select
    -- Primary identifiers
    contract_id,
    plan_id,
    segment_id,
    
    -- Coverage and tier information
    coverage_level as coverage_level_id,
    case coverage_level
        when 0 then 'pre-deductible'
        when 1 then 'initial coverage'
        when 3 then 'catastrophic'
    end as coverage_level,
    tier,
    days_supply as days_supply_id,
    case days_supply
        when 1 then '30'
        when 2 then '90'
        when 4 then '60'
        when 3 then 'other'
    end as days_supply,
    
    -- Preferred retail pharmacy cost sharing
    cost_type_pref as cost_type_pref_id,
    case cost_type_pref
        when 0 then 'not offered'
        when 1 then 'copay'
        when 2 then 'coinsurance'
    end as cost_type_pref,
    cast(cost_amt_pref as numeric) as cost_amt_pref,
    cast(cost_min_amt_pref as numeric) as cost_min_amt_pref,
    cast(cost_max_amt_pref as numeric) as cost_max_amt_pref,
    
    -- Non-preferred retail pharmacy cost sharing
    cost_type_nonpref as cost_type_nonpref_id,
    case cost_type_nonpref
        when 0 then 'not offered'
        when 1 then 'copay'
        when 2 then 'coinsurance'
    end as cost_type_nonpref,
    cast(cost_amt_nonpref as numeric) as cost_amt_nonpref,
    cast(cost_min_amt_nonpref as numeric) as cost_min_amt_nonpref,
    cast(cost_max_amt_nonpref as numeric) as cost_max_amt_nonpref,
    
    -- Preferred mail-order pharmacy cost sharing
    cost_type_mail_pref as cost_type_mail_pref_id,
    case cost_type_mail_pref
        when 0 then 'not offered'
        when 1 then 'copay'
        when 2 then 'coinsurance'
    end as cost_type_mail_pref,
    cast(cost_amt_mail_pref as numeric) as cost_amt_mail_pref,
    cast(cost_min_amt_mail_pref as numeric) as cost_min_amt_mail_pref,
    cast(cost_max_amt_mail_pref as numeric) as cost_max_amt_mail_pref,
    
    -- Non-preferred mail-order pharmacy cost sharing
    cost_type_mail_nonpref as cost_type_mail_nonpref_id,
    case cost_type_mail_nonpref
        when 0 then 'not offered'
        when 1 then 'copay'
        when 2 then 'coinsurance'
    end as cost_type_mail_nonpref,
    cast(cost_amt_mail_nonpref as numeric) as cost_amt_mail_nonpref,
    cast(cost_min_amt_mail_nonpref as numeric) as cost_min_amt_mail_nonpref,
    cast(cost_max_amt_mail_nonpref as numeric) as cost_max_amt_mail_nonpref,
    
    -- Tier and deductible flags
    tier_specialty_yn,
    ded_applies_yn

from {{ source('cms_part_d', 'cms_beneficiary_cost') }}
