SELECT fdae.recall_number, arr.id_value #>> '{}' AS app_num

FROM datasource.fda_enforcement fdae,
	json_array_elements(openfda->'package_ndc') with ordinality arr(id_value, line);