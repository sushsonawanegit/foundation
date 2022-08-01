

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
),ax_opco_item_size as(
    select 
    current_timestamp as crt_dtm,
    its._fivetran_synced as stg_load_dtm,
    case
        when its._fivetran_deleted = true then its._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(its .inventsizeid) as src_item_size_cd,
    its.dataareaid as opco_id,
    its.itemid as src_item_cd,
    oi.opco_item_sk as src_item_sk,
    its.name as src_item_size_nm,
    its.description as src_item_size_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTSIZE its 
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on its.dataareaid = oi.opco_id
        and its.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    where dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_size_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') as 
    varchar
)) as opco_item_size_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_size_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_sk as 
    varchar
), '') || '-' || coalesce(cast(src_item_size_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_size_desc as 
    varchar
), '') as 
    varchar
)) as opco_item_size_ck,
            * 
    from ax_opco_item_size 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_size ) 
    
