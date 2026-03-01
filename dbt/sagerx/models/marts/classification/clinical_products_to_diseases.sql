with

clinical_products_crosswalk as (
    
    select
        clinical_product_rxcui,
        clinical_product_name,
        clinical_product_tty,
        via_ingredient_rxcui,
        via_ingredient_name,
        via_ingredient_tty,
        rela,
        rela_source,
        class_id,
        class_name,
        class_type,
        to_code as disease_id,
        to_source as disease_source,
        to_name as disease_name        
    from {{ ref('int_umls_clinical_products_to_crosswalk_codes') }}

)

select * from clinical_products_crosswalk
