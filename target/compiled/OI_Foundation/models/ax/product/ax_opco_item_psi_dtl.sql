

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
),ax_opco_item_psi_dtl as (
    select
    current_timestamp as crt_dtm, 
    it._fivetran_synced as stg_load_dtm,
    case
        when it._fivetran_deleted = true then it._fivetran_synced
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    it.itemid as src_item_cd,
    it.dataareaid as opco_id,
    case 
        when it.moduletype =  0 then 'Inventory'
        when it.moduletype =  1 then 'Purchase Order'
        when it.moduletype =  2 then 'Sales Order'
    end as psi_type_txt,
    oi.opco_item_sk,
    ou.opco_uom_sk,
    it.pricedate as psi_price_dt,
    it.price as psi_price_amt,
    it.priceunit as psi_price_unit_qty,
    it.del_quantity as psi_dlvry_qty,
    it.linedisc as psi_line_dscnt_amt
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTTABLEMODULE it 
    left join __dbt__cte__v_ax_opco_item_curr oi 
        on it.dataareaid = oi.opco_id 
        and it.itemid = oi.src_item_cd
        and oi.src_sys_nm = 'AX'
    left join __dbt__cte__v_ax_opco_uom_curr ou 
        on upper(it.unitid) = ou.src_uom_cd
        and ou.src_sys_nm = 'AX'
    where it.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') || '-' || coalesce(cast(psi_type_txt as 
    varchar
), '') as 
    varchar
)) as opco_item_psi_dtl_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_item_cd as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(psi_type_txt as 
    varchar
), '') || '-' || coalesce(cast(opco_item_sk as 
    varchar
), '') || '-' || coalesce(cast(opco_uom_sk as 
    varchar
), '') || '-' || coalesce(cast(psi_price_dt as 
    varchar
), '') || '-' || coalesce(cast(psi_price_amt as 
    varchar
), '') || '-' || coalesce(cast(psi_price_unit_qty as 
    varchar
), '') || '-' || coalesce(cast(psi_dlvry_qty as 
    varchar
), '') || '-' || coalesce(cast(psi_line_dscnt_amt as 
    varchar
), '') as 
    varchar
)) as opco_item_psi_dtl_ck,
            concat_ws('|', src_sys_nm, opco_id, src_item_cd, psi_type_txt) as opco_item_psi_dtl_ak, 
            * 
    from ax_opco_item_psi_dtl 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_psi_dtl ) 
    
