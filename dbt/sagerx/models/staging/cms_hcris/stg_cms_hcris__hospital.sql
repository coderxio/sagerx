with

final as (
	
    select 
        rpt.prvdr_num as cms_id,
        rpt.npi,
        prvdr_ctrl_type_cd as ownership_code,
        case rpt.prvdr_ctrl_type_cd
            when '1' then 'voluntary nonprofit-church'
            when '2' then 'voluntary nonprofit-other'
            when '3' then 'proprietary-individual'
            when '4' then 'proprietary-corporation'
            when '5' then 'proprietary-partnership'
            when '6' then 'proprietary-other'
            when '7' then 'governmental-federal'
            when '8' then 'governmental-city-county'
            when '9' then 'governmental-county'
            when '10' then 'governmental-state'
            when '11' then 'governmental-hospital district'
            when '12' then 'governmental-city'
            when '13' then 'governmental-other'
            else 'unknown'
        end as ownership_type,
        name.itm_alphnmrc_itm_txt as hospital_name,
        street.itm_alphnmrc_itm_txt as street_address,
        city.itm_alphnmrc_itm_txt as city,
        state.itm_alphnmrc_itm_txt as state,
        zip.itm_alphnmrc_itm_txt as zip,
        hosp_beds.itm_val_num as hosp_bed_count,
        ld_beds.itm_val_num as ld_bed_count,
        coalesce(hosp_beds.itm_val_num, 0) + coalesce(ld_beds.itm_val_num, 0) as total_bed_count,
        residents.itm_val_num as resident_count,
        revenue.itm_val_num as total_revenue,
        net_rev.itm_val_num as net_revenue
    from sagerx_lake.cms_hcris_rpt rpt
    left join sagerx_lake.cms_hcris_alpha name 
        on rpt.rpt_rec_num = name.rpt_rec_num 
        and name.wksht_cd = 'S200001'
        and name.line_num = '00300'
        and name.clmn_num = '00100'
    left join sagerx_lake.cms_hcris_alpha street
        on rpt.rpt_rec_num = street.rpt_rec_num 
        and street.wksht_cd = 'S200001'
        and street.line_num = '00100'
        and street.clmn_num = '00100'
    left join sagerx_lake.cms_hcris_alpha city
        on rpt.rpt_rec_num = city.rpt_rec_num 
        and city.wksht_cd = 'S200001'
        and city.line_num = '00200'
        and city.clmn_num = '00100'
    left join sagerx_lake.cms_hcris_alpha state
        on rpt.rpt_rec_num = state.rpt_rec_num 
        and state.wksht_cd = 'S200001'
        and state.line_num = '00200'
        and state.clmn_num = '00200'
    left join sagerx_lake.cms_hcris_alpha zip
        on rpt.rpt_rec_num = zip.rpt_rec_num 
        and zip.wksht_cd = 'S200001'
        and zip.line_num = '00200'
        and zip.clmn_num = '00300'
    left join sagerx_lake.cms_hcris_nmrc hosp_beds
        on rpt.rpt_rec_num = hosp_beds.rpt_rec_num 
        and hosp_beds.wksht_cd = 'S300001'
        and hosp_beds.line_num = '01400'
        and hosp_beds.clmn_num = '00200'
    left join sagerx_lake.cms_hcris_nmrc ld_beds
        on rpt.rpt_rec_num = ld_beds.rpt_rec_num 
        and ld_beds.wksht_cd = 'S300001'
        and ld_beds.line_num = '03200'
        and ld_beds.clmn_num = '00200'
    left join sagerx_lake.cms_hcris_nmrc residents
        on rpt.rpt_rec_num = residents.rpt_rec_num 
        and residents.wksht_cd = 'S300001'
        and residents.line_num = '01400'
        and residents.clmn_num = '00900'
    left join sagerx_lake.cms_hcris_nmrc revenue
        on rpt.rpt_rec_num = revenue.rpt_rec_num 
        and revenue.wksht_cd = 'G300000'
        and revenue.line_num = '00200'
        and revenue.clmn_num = '00100'
    left join sagerx_lake.cms_hcris_nmrc net_rev
        on rpt.rpt_rec_num = net_rev.rpt_rec_num 
        and net_rev.wksht_cd = 'G300000'
        and net_rev.line_num = '00300'
        and net_rev.clmn_num = '00100'

)

select * from final
