

with  __dbt__cte__v_ax_opco_curr as (
with v_ax_opco as(
    select *,
    rank() over(partition by opco_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco
),
final as(
    select 
    *
    from v_ax_opco
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cust_curr as (
with v_ax_opco_cust as(
    select *,
    rank() over(partition by opco_cust_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust
),
final as(
    select 
    *
    from v_ax_opco_cust
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_cost_center_curr as (
with v_ax_opco_cost_center as(
    select *,
    rank() over(partition by opco_cost_center_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cost_center
),
final as(
    select 
    *
    from v_ax_opco_cost_center
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_dept_curr as (
with v_ax_opco_dept as(
    select *,
    rank() over(partition by opco_dept_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dept
),
final as(
    select 
    *
    from v_ax_opco_dept
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_type_curr as (
with v_ax_opco_type as(
    select *,
    rank() over(partition by opco_type_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type
),
final as(
    select 
    *
    from v_ax_opco_type
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_purpose_curr as (
with v_ax_opco_purpose as(
    select *,
    rank() over(partition by opco_purpose_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_purpose
),
final as(
    select 
    *
    from v_ax_opco_purpose
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_lob_curr as (
with v_ax_opco_lob as(
    select *,
    rank() over(partition by opco_lob_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob
),
final as(
    select 
    *
    from v_ax_opco_lob
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_pymnt_terms_curr as (
with v_ax_opco_pymnt_terms as(
    select *,
    rank() over(partition by opco_pymnt_terms_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_pymnt_terms
),
final as(
    select 
    *
    from v_ax_opco_pymnt_terms
    where rnk = 1 and delete_dtm is null
)

select * from final
),  __dbt__cte__v_ax_opco_locn_curr as (
with v_ax_opco_locn as(
    select *,
    rank() over(partition by opco_locn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn
),
final as(
    select 
    *
    from v_ax_opco_locn
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_project as(
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJTABLE pt
    left join __dbt__cte__v_ax_opco_curr o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join __dbt__cte__v_ax_opco_cust_curr oc 
        on pt.dataareaid = oc.opco_id
		and pt.custaccount = oc.cust_id
        and oc.src_sys_nm = 'AX'  		
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pt.dimension) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od
        on upper(pt.dimension2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pt.dimension3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_purpose_curr op
        on upper(pt.dimension4_) = op.src_purpose_cd	
        and op.src_sys_nm = 'AX'  		
	left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(pt.dimension5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_pymnt_terms_curr opt 
        on upper(pt.payment_opi) = opt.src_pymnt_terms_cd	
        and opt.src_sys_nm = 'AX'  
	left join __dbt__cte__v_ax_opco_locn_curr olc 
		on upper(pt.deliverystreet) = olc.addr_ln_1_txt 
		and upper(pt.deliverycity) = olc.city_nm
		and upper(pt.dlvstate) = olc.state_nm
		and upper(pt.dlvcountryregionid) = olc.country_nm
		and upper(pt.dlvzipcode) = olc.zip_cd
        and olc.src_sys_nm = 'AX'
	where pt.dataareaid not in ('044', '045', '047', '999') 
),
final as (															
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_id as 
    varchar
), '') as 
    varchar
)) as opco_project_sk,		
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(project_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cust_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_dept_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_type_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_purpose_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_lob_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_pymnt_terms_sk as 
    varchar
), '') || '-' || coalesce(cast(dlvry_locn_sk as 
    varchar
), '') || '-' || coalesce(cast(project_nm as 
    varchar
), '') || '-' || coalesce(cast(project_status_txt as 
    varchar
), '') || '-' || coalesce(cast(project_type_txt as 
    varchar
), '') || '-' || coalesce(cast(project_crt_dt as 
    varchar
), '') || '-' || coalesce(cast(project_start_dt as 
    varchar
), '') || '-' || coalesce(cast(project_end_dt as 
    varchar
), '') || '-' || coalesce(cast(estimated_start_dt as 
    varchar
), '') || '-' || coalesce(cast(estimated_end_dt as 
    varchar
), '') || '-' || coalesce(cast(actual_start_dt as 
    varchar
), '') || '-' || coalesce(cast(actual_end_dt as 
    varchar
), '') || '-' || coalesce(cast(estimator_nm as 
    varchar
), '') || '-' || coalesce(cast(project_mgr_nm as 
    varchar
), '') || '-' || coalesce(cast(gl_posting_ind as 
    varchar
), '') || '-' || coalesce(cast(sort_1_id as 
    varchar
), '') || '-' || coalesce(cast(sort_2_id as 
    varchar
), '') || '-' || coalesce(cast(sort_3_id as 
    varchar
), '') || '-' || coalesce(cast(sls_grp_txt as 
    varchar
), '') || '-' || coalesce(cast(tax_grp_id as 
    varchar
), '') || '-' || coalesce(cast(locn_1_txt as 
    varchar
), '') || '-' || coalesce(cast(locn_2_txt as 
    varchar
), '') || '-' || coalesce(cast(retention_terms_txt as 
    varchar
), '') || '-' || coalesce(cast(retention_due_dt as 
    varchar
), '') || '-' || coalesce(cast(use_tax_ind as 
    varchar
), '') as 
    varchar
)) as opco_project_ck,
            concat_ws('|', src_sys_nm, opco_id, project_id) as opco_project_ak,
            * 		
    from ax_opco_project 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project ) 
    
