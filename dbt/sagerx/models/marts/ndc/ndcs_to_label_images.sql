with

images as (

	select * from {{ ref('int_dailymed_ranked_package_label_images') }}
),

ndcs as (

	select * from {{ ref('int_dailymed_ranked_package_label_ndcs') }}
),

ndcs_to_products as (

    select * from {{ ref('int_rxnorm_ndcs_to_products') }}

),

ndcs_to_label_images as (

	select
		img.set_id,
		ndc.ndc,
		{{ ndc_to_11('ndc.ndc') }} as ndc11,
		img.image,
		concat('https://dailymed.nlm.nih.gov/dailymed/image.cfm?name=', img.image, '&setid=', img.set_id, '&type=img') as image_url,
		concat('https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid=', img.set_id) as spl_url
	from images img
	left join ndcs ndc
		on ndc.package_label_section_id = img.package_label_section_id
		and ndc.rn = img.rn
	where ndc.ndc is not null

),

ndcs_to_label_images_with_product_rxcuis as (

	select
		ndcs_to_label_images.*,		
		ndcs_to_products.product_rxcui
	from ndcs_to_label_images
	inner join ndcs_to_products
		on ndcs_to_products.ndc = ndcs_to_label_images.ndc11

)

select * from ndcs_to_label_images_with_product_rxcuis