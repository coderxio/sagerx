-- stg_umls__crosswalk_codes.sql

select
    -- TODO: make DAG store the source name (MSH)
    -- so this is more general than just MeSH
    'MSH' as from_source,
    mesh_code as from_code,
    root_source as to_source,
    ui as to_code,
    name as to_name
from {{ source('umls', 'umls_crosswalk') }}
where obsolete = false
