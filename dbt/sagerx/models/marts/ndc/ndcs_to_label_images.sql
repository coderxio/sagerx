-- ndcs_to_label_images

with

image_xml_ndcs as (

	select * from {{ ref('int_dailymed_image_xml_ndcs') }}


),

image_name_ndcs as (

	select * from {{ ref('int_dailymed_image_name_ndcs') }}

),

all_image_ndcs as (

	select
		set_id,
		ndc,
		image	
	from image_xml_ndcs

	union

	select
		set_id,
		ndc,
		image
	from image_name_ndcs

),

all_image_ndcs_ndc11 as (

	select
		set_id,
		ndc,
		{{ ndc_to_11('ndc') }} as ndc11,
		concat('https://dailymed.nlm.nih.gov/dailymed/image.cfm?name=', image, '&setid=', set_id) as image_url,
		image as image_file,
		concat('https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid=', set_id) as dailymed_spl_url
	from all_image_ndcs

)

select * from all_image_ndcs_ndc11
