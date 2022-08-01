

with ns_opco_uom as (
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    uom_id::varchar(15) as src_uom_cd,
    name::varchar(50) as src_uom_desc
    from FIVETRAN.NETSUITE.UOM
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_uom_cd as 
    varchar
), '') as 
    varchar
)) as opco_uom_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_uom_cd as 
    varchar
), '') || '-' || coalesce(cast(src_uom_desc as 
    varchar
), '') as 
    varchar
)) as opco_uom_ck,
            * 
    from ns_opco_uom
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_uom ) 
    
