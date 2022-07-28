{% set _load = load_type('AX_OPCO_PROJECT_CATGRY') %}

with ax_opco_project_catgry as(
    select 
    current_timestamp as crt_dtm,
    pt._fivetran_synced as stg_load_dtm,
	case 
        when pt._fivetran_deleted = true then pt._fivetran_synced
        else null
	end as delete_dtm,
	'AX' as src_sys_nm,
	pt.dataareaid as opco_id,
	pt.categoryid as catgry_cd,
	o.opco_sk,
	oi.opco_item_sk,
	occ.opco_cost_center_sk,
	od.opco_dept_sk,
	ot.opco_type_sk,
	op.opco_purpose_sk,
	ol.opco_lob_sk,
	ou.opco_uom_sk,
	pt.name as catgry_nm,
	pt.categorygroupid as catgry_grp_cd,
	case 
        when pt.categorytype = 0 then 'None'
        when pt.categorytype = 1 then 'Fee'
        when pt.categorytype = 2 then 'Hour'
        when pt.categorytype = 3 then 'Expense'
        when pt.categorytype = 4 then 'Item'
    end as catgry_type_txt,
	case 
        when pt.projcategoryemploption = 0 then 'Mandatory'
        when pt.projcategoryemploption = 1 then 'Optional'
        when pt.projcategoryemploption = 2 then 'Never'
    end as empl_applicable_ind,
	pt.active as actv_ind
    from {{source('AX_DEV', 'PROJCATEGORY')}} pt
    left join {{ref('opco')}} o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join {{ref('opco_item')}} oi 
        on pt.dataareaid = oi.opco_id
		and pt.itemid_opi = oi.src_item_cd
        and oi.src_sys_nm = 'AX'  	
    left join {{ref('opco_cost_center')}} occ 
        on upper(pt.dimension_opi) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join {{ref('opco_dept')}} od
        on upper(pt.dimension_opi2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join {{ref('opco_type')}} ot 
        on upper(pt.dimension_opi3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join {{ref('opco_purpose')}} op
        on upper(pt.dimension_opi4_) = op.src_purpose_cd	
        and op.src_sys_nm = 'AX'  		
	left join {{ref('opco_lob')}} ol 
        on upper(pt.dimension_opi5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join {{ref('opco_uom')}} ou 
        on upper(pt.unitid_opi) = ou.src_uom_cd	
        and ou.src_sys_nm = 'AX'  
    where pt.dataareaid not in {{ var('excluded_ax_companies')}} 
),
final as (															
    select  {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','catgry_cd']) }} as opco_project_catgry_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm','opco_id','catgry_cd', 'opco_sk', 'opco_item_sk', 'opco_cost_center_sk', 'opco_dept_sk', 'opco_type_sk', 'opco_purpose_sk', 'opco_lob_sk', 'opco_uom_sk', 'catgry_nm', 'catgry_grp_cd', 'catgry_type_txt', 'empl_applicable_ind', 'actv_ind']) }} as opco_project_catgry_ck,
            * 		
    from ax_opco_project_catgry 	
)

select * from final
{% if tb_check(var('fnd_tbl_db'), var('intermediate_tbl_sch'), 'AX_OPCO_PROJECT_CATGRY') and tb_check(var('fnd_tbl_db'), var('fnd_tbl_sch'), 'OPCO_PROJECT_CATGRY') and _load != 3%}
    union
    select
    opco_project_catgry_sk, opco_project_catgry_ck, crt_dtm, 
    (select max(stg_load_dtm) from final) as stg_load_dtm,
    (select max(stg_load_dtm) from final) as delete_dtm,
    src_sys_nm, opco_id, catgry_cd, opco_sk, opco_item_sk, opco_cost_center_sk, opco_dept_sk, opco_type_sk, opco_purpose_sk, opco_lob_sk, opco_uom_sk, catgry_nm, catgry_grp_cd, catgry_type_txt, empl_applicable_ind, actv_ind
    from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT_CATGRY
    {% if _load == 1 %}
        where opco_PROJECT_CATGRY_ck not in (select distinct opco_PROJECT_CATGRY_ck from final)
    {% else %}
        where opco_PROJECT_CATGRY_ck not in (select distinct opco_PROJECT_CATGRY_ck from final) 
            and date(stg_load_dtm) = (select date(max(stg_load_dtm)) from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT_CATGRY where src_sys_nm = 'AX')
    {% endif %} 
    and src_sys_nm = 'AX'
{% endif %}


{% if is_incremental() %}
    {% if _load == 3 %}
        where stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) 
    {% else %}
        where 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_PROJECT_CATGRY_ck not in (select distinct opco_PROJECT_CATGRY_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT_CATGRY where src_sys_nm = 'AX')) 
            or 
            (stg_load_dtm > (select max(stg_load_dtm) from {{this}} ) and opco_PROJECT_CATGRY_ck in (select distinct opco_PROJECT_CATGRY_ck from {{ var('fnd_tbl_db')}}.{{ var('fnd_tbl_sch')}}.opco_PROJECT_CATGRY where src_sys_nm = 'AX') and delete_dtm is not null)
    {% endif %}
{% endif %}														