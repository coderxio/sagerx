 /* stg_dailymed__package_label_section_images */

with

package_label_sections as
(
	select * from {{ ref('stg_dailymed__package_label_sections') }}
),

images as (

	select
		p.set_id,
		p.id as package_label_section_id,
		y.*
	from package_label_sections p,
		xmltable(
			'//MediaList/Media' passing media_list
			columns 
				image 		text  path 'Image',
				image_id  	text  path 'ID'
		) y

),

id_images as (

	select
		row_number() over() as id,
		*
	from images

)

select * from id_images
