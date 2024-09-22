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

)

select * from all_image_ndcs
