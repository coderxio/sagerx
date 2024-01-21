-- DISCLAIMER: because I can't figure out NDC-level mapping, I only include applications with a single OB TE code
WITH cte AS (
    SELECT
        fda.ndc11
        , obp.te_code
        , COUNT(fda.ndc11) OVER( PARTITION BY fda.ndc11 ) AS num_te_codes
    FROM {{ source('orange_book', 'orange_book_products') }} AS obp
    INNER JOIN {{ ref('staging.stg_fda_ndc__ndc') }} AS fda 
        ON concat(CASE WHEN obp.appl_type = 'A' THEN 'ANDA' ELSE 'NDA' END, obp.appl_no) = fda.applicationnumber
    GROUP BY fda.ndc11, obp.te_code
)
SELECT
    fda.ndc11
    , fda.applicationnumber AS application_number
    , cte.te_code
    , LEFT(cte.te_code, 2) AS first_two_te_code
    , LEFT(cte.te_code, 1) AS first_one_te_code
FROM {{ ref('staging.stg_fda_ndc__ndc') }} AS fda 
INNER JOIN cte 
    ON fda.ndc11 = cte.ndc11 
    AND cte.num_te_codes = 1