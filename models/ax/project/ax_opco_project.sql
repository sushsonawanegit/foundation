{% set _load = load_type('AX_OPCO_PROJECT') %}

with ax_opco_project as(
    select 
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
	case 
        when pt._fivetran_deleted = true then pt._fivetran_synced
        else null
	end as delete_dtm,
	'AX' as src_sys_nm,
	pt.dataareaid as opco_id,
	pt.projid as project_id,
	o.opco_sk,
	oc.opco_cust_sk,
	occ.opco_cost_center_sk,
	od.opco_dept_sk,
	ot.opco_type_sk,
	op.opco_purpose_sk,
	ol.opco_lob_sk,
	opt.opco_pymnt_terms_sk,
	olc.opco_locn_sk as dlvry_locn_sk,
	pt.name as project_nm,
	case 
        when pt.status = 0 then 'Created'
        when pt.status = 1 then 'Estimated'
        when pt.status = 2 then 'Scheduled'
        when pt.status = 3 then 'In Process'
        when pt.status = 4 then 'Finished'
    end as project_status_txt,
	case 
        when pt.type = 0 then 'Time and Material'
        when pt.type = 1 then 'Fixed Price'
        when pt.type = 2 then 'Investment'
		when pt.type = 3 then 'Cost Project'
        when pt.type = 4 then 'Internal'
        when pt.type = 5 then 'Time Project'
		when pt.type = 6 then 'Summary'
    end as project_type_txt,
	pt.created as  project_crt_dt,     
	pt.startdate as project_start_dt ,  
	pt.enddate as project_end_dt ,    
	pt.estimatestartdate_opi as estimated_start_dt, 
	pt.estimateenddate_opi  as estimated_end_dt,   
	pt.actualstartdate_opi as actual_start_dt,    
	pt.actualenddate_opi as actual_end_dt,      
	pt.estimator_opi as estimator_nm,       
	pt.projmanager_opi  as project_mgr_nm,     
	pt.projledgerposting  as gl_posting_ind,     
	pt.sortingid as sort_1_id,          
	pt.sortingid2_ as sort_2_id,          
	pt.sortingid3_ as sort_3_id,          
	pt.salesgroup_opi  as sls_grp_txt,        
	pt.taxgroupid as tax_grp_id,         
	pt.location1_opi as locn_1_txt ,         
	pt.location2_opi as locn_2_txt ,         
	pt.retentionterms_opi as retention_terms_txt ,
	pt.retentionduedate_opi as retention_due_dt ,   
	pt.payusetax_opi as use_tax_ind        
    from {{source('AX_DEV', 'PROJTABLE')}} pt
    left join {{ref('opco')}} o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join {{ref('opco_cust')}} oc 
        on pt.dataareaid = oc.opco_id
		and pt.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'  		
    left join {{ref('opco_cost_center')}} occ 
        on upper(pt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join {{ref('opco_type')}} ot 
        on upper(pt.dimension3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join {{ref('opco_purpose')}} op
        on upper(pt.dimension4_) = op.src_purpose_cd	
        and op.src_sys_nm = 'AX'  		
	left join {{ref('opco_lob')}} ol 
        on upper(pt.dimension5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join {{ref('opco_pymnt_terms')}} opt 
        on upper(pt.payment_opi) = opt.src_pymnt_terms_cd	
        and opt.src_sys_nm = 'AX'  
	left join {{ ref('opco_locn')}} olc 
		on upper(pt.deliverystreet) = olc.addr_ln_1_txt 
		and upper(pt.deliverycity) = olc.city_nm
		and upper(pt.dlvstate) = olc.state_nm
		and upper(pt.dlvcountryregionid) = olc.country_nm
		and upper(pt.dlvzipcode) = olc.zip_cd
        and olc.src_sys_nm = 'AX'
	where pt.dataareaid not in {{ var('excluded_ax_companies')}} 
),
final as (															
    select  {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','project_id']) }} as opco_project_sk,		
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'opco_id', 'project_id', 'opco_sk', 'opco_cust_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_pymnt_terms_sk', 'dlvry_locn_sk', 'project_nm', 'project_status_txt', 'project_type_txt', 'project_crt_dt', 'project_start_dt', 'project_end_dt', 'estimated_start_dt', 'estimated_end_dt', 'actual_start_dt', 'actual_end_dt', 'estimator_nm', 'project_mgr_nm', 'gl_posting_ind', 'sort_1_id', 'sort_2_id', 'sort_3_id', 'sls_grp_txt', 'tax_grp_id', 'locn_1_txt', 'locn_2_txt', 'retention_terms_txt', 'retention_due_dt', 'use_tax_ind']) }} as opco_project_ck,
            concat_ws('|', src_sys_nm, opco_id, project_id) as opco_project_ak,
            * 		
    from ax_opco_project 
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT') and _load != 3%}
    union
    select
    opco_PROJECT_sk, opco_PROJECT_ck, OPCO_PROJECT_AK, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, project_id, opco_sk, opco_cust_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_pymnt_terms_sk, dlvry_locn_sk, project_nm, project_status_txt, project_type_txt, project_crt_dt, project_start_dt, project_end_dt, estimated_start_dt, estimated_end_dt, actual_start_dt, actual_end_dt, estimator_nm, project_mgr_nm, gl_posting_ind, sort_1_id, sort_2_id, sort_3_id, sls_grp_txt, tax_grp_id, locn_1_txt, locn_2_txt, retention_terms_txt, retention_due_dt, use_tax_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT
    {% if _load == 1 %}
        where opco_PROJECT_ck not in (select distinct opco_PROJECT_ck from final)
    {% else %}
        where opco_PROJECT_ck not in (select distinct opco_PROJECT_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_PROJECT_ck not in (select distinct opco_PROJECT_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_PROJECT_ck in (select distinct opco_PROJECT_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}