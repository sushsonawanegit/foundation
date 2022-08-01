

with ax_opco_dlvry_mode as(
    select 
    current_timestamp as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(code) as src_dlvry_mode_cd,
    max(txt) as src_dlvry_mode_desc
    from FIVETRAN.AX_PRODREPLICATION1_DBO.DLVMODE
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_dlvry_mode_cd, _fivetran_deleted
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_dlvry_mode_cd as 
    varchar
), '') as 
    varchar
)) as opco_dlvry_mode_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_dlvry_mode_cd as 
    varchar
), '') || '-' || coalesce(cast(src_dlvry_mode_desc as 
    varchar
), '') as 
    varchar
)) as opco_dlvry_mode_ck, 
        *
    from ax_opco_dlvry_mode
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode ) 
    
