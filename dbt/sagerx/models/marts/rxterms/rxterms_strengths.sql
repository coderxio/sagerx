SELECT DISTINCT
    rxcui
    , display_name AS name
    , CONCAT(strength, ' ', new_dose_form) AS strength
FROM {{ source('rxterms','rxterms')}}
WHERE suppress_for IS NULL 
    AND is_retired IS NULL