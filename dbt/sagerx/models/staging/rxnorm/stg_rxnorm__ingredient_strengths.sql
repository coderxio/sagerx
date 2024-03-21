-- stg_rxnorm__ingredient_strengths.sql

select
	ingredient_strength.rxcui as rxcui
	, ingredient_strength.str as name
	, numerator_value.atv as numerator_value
	, numerator_unit.atv as numerator_unit
	, denominator_value.atv as denominator_value
	, denominator_unit.atv as denominator_unit
	, text.atv as text
	, case when ingredient_strength.suppress = 'N'
        then true
        else false
        end as active
	, case when ingredient_strength.cvf = '4096'
        then true
        else false
        end as prescribable
from sagerx_lake.rxnorm_rxnconso ingredient_strength
left join sagerx_lake.rxnorm_rxnsat numerator_value
    on numerator_value.rxcui = ingredient_strength.rxcui
    and numerator_value.atn = 'RXN_BOSS_STRENGTH_NUM_VALUE'
left join sagerx_lake.rxnorm_rxnsat numerator_unit
    on numerator_unit.rxcui = ingredient_strength.rxcui
    and numerator_unit.atn = 'RXN_BOSS_STRENGTH_NUM_UNIT'
left join sagerx_lake.rxnorm_rxnsat denominator_value
    on denominator_value.rxcui = ingredient_strength.rxcui
    and denominator_value.atn = 'RXN_BOSS_STRENGTH_DENOM_VALUE'
left join sagerx_lake.rxnorm_rxnsat denominator_unit
    on denominator_unit.rxcui = ingredient_strength.rxcui
    and denominator_unit.atn = 'RXN_BOSS_STRENGTH_DENOM_UNIT'
left join sagerx_lake.rxnorm_rxnsat text
    on text.rxcui = ingredient_strength.rxcui
    and text.atn = 'RXN_STRENGTH'
where ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM'
