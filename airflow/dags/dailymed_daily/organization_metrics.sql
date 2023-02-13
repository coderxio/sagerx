SELECT o.set_id
	,ma.market_status
	,SUM(CASE WHEN org_type = 'Functioner' THEN 1 ELSE 0 END) AS Functioner_count
	,SUM(CASE WHEN org_type = 'Labeler' THEN 1 ELSE 0 END) AS Labeler_count
	,SUM(CASE WHEN org_type = 'Repacker' THEN 1 ELSE 0 END) AS Repacker_count
	,CASE WHEN SUM(CASE WHEN ot.set_id IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 'Yes' ELSE '' END AS organization_text
	,CASE WHEN SUM(CASE WHEN org_type = 'Labeler' THEN 1 ELSE 0 END) = 1 
				AND SUM(CASE WHEN org_type = 'Functioner' THEN 1 ELSE 0 END) = 0
			THEN 'Yes' ELSE '' END AS labeler_only
	,COUNT(*)

FROM staging.dailymed_organization o
	INNER JOIN staging.dailymed_main ma ON o.set_id = ma.set_id
	LEFT JOIN staging.dailymed_organization_text ot ON o.set_id = ot.set_id

GROUP BY o.set_id, ma.market_status