select
	r.rxcui,
	a.atc_code,
	r.str as name,
	r.tty,
	a.ddd,
	a.uom,
	a.adm_route,
	a.note
from {{ source('atc_ddd', 'atc_ddd') }} a 
join {{ source('rxnorm_rxnconso', 'rxnorm_rxnconso') }} r on a.atc_code = r.code
where r.sab = 'ATC'