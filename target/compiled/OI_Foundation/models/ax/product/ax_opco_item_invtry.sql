

with  __dbt__cte__v_ax_opco_item_curr as (
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
),  __dbt__cte__v_ax_opco_assctn_curr as (
with v_ax_opco_assctn as(
    select *,
    rank() over(partition by opco_assctn_sk order by stg_load_dtm desc, delete_dtm desc) as rnk
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn
),
final as(
    select 
    *
    from v_ax_opco_assctn
    where rnk = 1 and delete_dtm is null
)

select * from final
),ax_opco_item_invtry as (
    select 
    oi.opco_item_sk,
    oa.opco_assctn_sk,
    current_timestamp as crt_dtm,
    isu._fivetran_synced as stg_load_dtm,
    case
        when isu._fivetran_deleted = true then isu._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    isu.availphysical as item_avbl_physical_qty,
    isu.postedqty as item_posted_qty,
    isu.postedvalue as item_posted_value_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTSUM isu 
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on isu.dataareaid = oi.opco_id
        and isu.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_assctn_curr oa 
        on isu.dataareaid = oa.opco_id 
        and isu.inventdimid = oa.src_assctn_cd
        and oa.src_sys_nm = 'AX'
    where isu.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            opco_item_sk, opco_assctn_sk, 
            md5(cast(coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_assctn_sk as 
    varchar
), '') || '-' || coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(item_avbl_physical_qty as 
    varchar
), '') || '-' || coalesce(cast(item_posted_qty as 
    varchar
), '') || '-' || coalesce(cast(item_posted_value_amt as 
    varchar
), '') as 
    varchar
)) as opco_item_invtry_ck,
            crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, item_avbl_physical_qty, item_posted_qty, item_posted_value_amt
    from ax_opco_item_invtry 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_invtry ) 
    
