/* sagerx_lake.nber_fips_crosswalk */
DROP TABLE IF EXISTS sagerx_lake.nber_fips_crosswalk CASCADE;

CREATE TABLE sagerx_lake.nber_fips_crosswalk (
    fipscounty        TEXT,
    countyname_fips   TEXT,
    state             TEXT,
    cbsa_code         TEXT,
    cbsa_name         TEXT,
    ssa_code          TEXT,
    state_name        TEXT,
    countyname_rate   TEXT
);

COPY sagerx_lake.nber_fips_crosswalk
FROM '{data_path}' DELIMITER ',' CSV HEADER ENCODING 'UTF8';

CREATE INDEX IF NOT EXISTS x_fipscounty
ON sagerx_lake.nber_fips_crosswalk(fipscounty);

CREATE INDEX IF NOT EXISTS x_ssa_code
ON sagerx_lake.nber_fips_crosswalk(ssa_code);

CREATE INDEX IF NOT EXISTS x_state
ON sagerx_lake.nber_fips_crosswalk(state);
