

with ax_opco_item_freight as (
    select 
    current_timestamp as crt_dtm,
    it._fivetran_synced as stg_load_dtm,
    case
        when it._fivetran_deleted = true then it._fivetran_synced 
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(it.itemgroupid) as src_item_freight_cd,
    it.dataareaid as opco_id,
    it.itemname as src_item_freight_nm
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTTABLE it
    join FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTITEMGROUP itg 
        on it.dataareaid = itg.dataareaid
        and it.itemid = itg.ifitemid_opi
        and itg._fivetran_deleted = false
    where it.dataareaid not in ('044', '045', '047', '999')
),
final as (
    select distinct 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_freight_cd as 
    varchar
), '') as 
    varchar
)) as opco_item_freight_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(src_item_freight_cd as 
    varchar
), '') || '-' || coalesce(cast(src_item_freight_nm as 
    varchar
), '') as 
    varchar
)) as OPCO_ITEM_FREIGHT_ck,
            * 
    from ax_opco_item_freight
)
    
select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_freight ) 
    
