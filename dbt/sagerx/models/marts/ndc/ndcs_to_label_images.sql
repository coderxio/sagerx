select
	img.set_id,
	ndc.ndc,
	img.image
from sagerx_dev.int_dailymed_ranked_package_label_images img
left join sagerx_dev.int_dailymed_ranked_package_label_ndcs ndc
	on ndc.package_label_section_id = img.package_label_section_id
	and ndc.rn = img.rn
where ndc.ndc is not null
