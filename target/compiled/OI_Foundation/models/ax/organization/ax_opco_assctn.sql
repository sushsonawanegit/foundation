

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
),  __dbt__cte__v_ax_opco_site_curr as (
with v_ax_opco_site as(
    select *,
    rank() over(partition by opco_site_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site
),
final as(
    select 
    *
    from v_ax_opco_site
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
),  __dbt__cte__v_ax_warehouse_curr as (
with v_ax_warehouse as(
    select *,
    rank() over(partition by warehouse_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse
),
final as(
    select 
    *
    from v_ax_warehouse
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_assctn as (
    select 
    current_timestamp as crt_dtm,
    id._fivetran_synced as stg_load_dtm,
    case
        when id._fivetran_deleted = true then id._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    id.inventdimid as src_assctn_cd,
    id.dataareaid as opco_id,
    opco.opco_sk,
    site.opco_site_sk,
    cc.opco_cost_center_sk as cost_center_sk,
    wr.warehouse_sk,
    1 as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTDIM id 
    left join __dbt__cte__v_ax_opco_curr opco
        on id.dataareaid = opco.opco_id
        and opco.src_sys_nm = 'AX' 
    left join __dbt__cte__v_ax_opco_site_curr site
        on upper(id.inventsiteid) = site.src_site_id
        and site.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_cost_center_curr cc
        on upper(id.inventsiteid) = cc.src_cost_center_cd
        and cc.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_warehouse_curr wr 
        on upper(id.inventlocationid) = wr.warehouse_id
        and id.dataareaid = wr.opco_id
        and wr.src_sys_nm = 'AX'
    where id.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_assctn_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') as 
    varchar
)) as opco_assctn_sk,
    md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_assctn_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(opco_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_site_sk as 
    varchar
), '') || '-' || coalesce(cast(cost_center_sk as 
    varchar
), '') || '-' || coalesce(cast(warehouse_sk as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_assctn_ck, 
    * 
    from ax_opco_assctn 
)


select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn ) 
    
