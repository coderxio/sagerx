-- int_dailymed_image_xml_ndcs

with

ranked_package_label_images as
(

    select * from {{ ref('int_dailymed_ranked_package_label_images') }}

),

ranked_package_label_ndcs as
(

    select * from {{ ref('int_dailymed_ranked_package_label_ndcs') }}

)

select
	img.set_id,
	ndc.ndc,
	img.image
from ranked_package_label_images img
left join ranked_package_label_ndcs ndc
	on ndc.package_label_section_id = img.package_label_section_id
	and ndc.rn = img.rn
where ndc.ndc is not null
