DROP TABLE IF EXISTS public.julius_hcpcs_corrections_2020 CASCADE;

CREATE TABLE public.julius_hcpcs_corrections_2020 (
    hcpcs_code TEXT,
    description TEXT,
    action TEXT
);
