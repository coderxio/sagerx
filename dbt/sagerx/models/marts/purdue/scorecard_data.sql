with

ing_to_report as (

    select * from {{ ref('int_inactive_ingredients_to_fda_enforcement_reports') }}

)

select * from ing_to_report
where active_ingredient_name in (
    'risperidone'
    , 'adalimumab'
    , 'lidocaine'
    , 'carbamazepine'
    , 'phenytoin'
    , 'midazolam'
    , 'valproate'
    , 'tacrolimus'
    , 'amoxicillin'
    , 'hydrocortisone'
    , 'cetirizine'
    , 'pertuzumab'
    , 'methylphenidate'
    , 'erythromycin'
    , 'gabapentin'
    , 'lopinavir / ritonavir'
    , 'levothyroxine'
    , 'albuterol'
    )
