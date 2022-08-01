

with ax_warehouse as(
    SELECT
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case 
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    dataareaid as opco_id,
    upper(inventlocationid) as warehouse_id,
    name as warehouse_nm,
    1 as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTLOCATION
    where dataareaid not in ('044', '045', '047', '999')
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(warehouse_id as 
    varchar
), '') as 
    varchar
)) as warehouse_sk,
    md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(opco_id as 
    varchar
), '') || '-' || coalesce(cast(warehouse_id as 
    varchar
), '') || '-' || coalesce(cast(warehouse_nm as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as warehouse_ck, 
    * 
    from ax_warehouse  
)


select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse ) 
    
