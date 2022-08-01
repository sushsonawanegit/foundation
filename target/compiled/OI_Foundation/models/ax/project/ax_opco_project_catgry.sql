

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
),  __dbt__cte__v_ax_opco_item_curr as (
with v_ax_opco_item as(
    select *,
    rank() over(partition by opco_item_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item
),
final as(
    select 
    *
    from v_ax_opco_item
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
),  __dbt__cte__v_ax_opco_uom_curr as (
with v_ax_opco_uom as(
    select *,
    rank() over(partition by opco_uom_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_uom
),
final as(
    select 
    *
    from v_ax_opco_uom
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_project_catgry as(
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
    from FIVETRAN.AX_PRODREPLICATION1_DBO.PROJCATEGORY pt
    left join __dbt__cte__v_ax_opco_curr o    
        on pt.dataareaid = o.opco_id    
        and o.src_sys_nm = 'AX'  
	left join __dbt__cte__v_ax_opco_item_curr oi 
        on pt.dataareaid = oi.opco_id
		and pt.itemid_opi = oi.src_item_cd
        and oi.src_sys_nm = 'AX'  	
    left join __dbt__cte__v_ax_opco_cost_center_curr occ 
        on upper(pt.dimension_opi) = occ.src_cost_center_cd
        and occ.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_dept_curr od
        on upper(pt.dimension_opi2_) = od.src_dept_cd
        and od.src_sys_nm = 'AX'
	left join __dbt__cte__v_ax_opco_type_curr ot 
        on upper(pt.dimension_opi3_) = ot.src_type_cd	
        and ot.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_purpose_curr op
        on upper(pt.dimension_opi4_) = op.src_purpose_cd	
        and op.src_sys_nm = 'AX'  		
	left join __dbt__cte__v_ax_opco_lob_curr ol 
        on upper(pt.dimension_opi5_) = ol.src_lob_cd	
        and ol.src_sys_nm = 'AX'  	
	left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(pt.unitid_opi) = ou.src_uom_cd	
        and ou.src_sys_nm = 'AX'  
    where pt.dataareaid not in ('044', '045', '047', '999') 
),
final as (															
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(catgry_cd as 
    varchar
), '') as 
    varchar
)) as opco_project_catgry_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(catgry_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
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
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(catgry_nm as 
    varchar
), '') || '-' || coalesce(cast(catgry_grp_cd as 
    varchar
), '') || '-' || coalesce(cast(catgry_type_txt as 
    varchar
), '') || '-' || coalesce(cast(empl_applicable_ind as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_project_catgry_ck,
            * 		
    from ax_opco_project_catgry 	
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_catgry ) 
    
