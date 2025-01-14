-- stg_fda_unii__unii_codes.sql

with

fda_unii as (
    select * from {{ source('fda_unii', 'fda_unii') }}
)

select
    unii
    , display_name
    , rxcui
    , pubchem
    , rn
    , ncit
    , ncbi
    , dailymed
from fda_unii
