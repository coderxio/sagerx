-- stg_ncpdp__terminology.sql
-- NCPDP terminology: DEA Schedule, Quantity/Dose/Strength units, dosage forms, etc.

with

ncpdp as (
    select * from {{ source('ncpdp', 'ncpdp') }}
)

select
    ncit_subset_code
    , ncpdp_subset_preferred_term
    , ncit_code
    , ncpdp_preferred_term
    , ncpdp_synonym
    , ncit_preferred_term
    , nci_definition
from ncpdp
