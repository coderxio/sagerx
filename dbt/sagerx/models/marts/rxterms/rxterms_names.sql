SELECT DISTINCT
    display_name AS name
    , display_name_synonym AS synonyms
FROM {{ source('rxterms','rxterms')}}
WHERE suppress_for IS NULL 
    AND is_retired IS NULL
