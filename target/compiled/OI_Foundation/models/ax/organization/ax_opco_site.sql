

with ax_opco_site as(
    SELECT 
    current_timestamp() as crt_dtm,
    max(_fivetran_synced) as stg_load_dtm,
    case
        when max(_fivetran_deleted) = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX' as src_sys_nm,
    upper(siteid) as src_site_id,
    max(name) as src_site_nm,
    1 as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.INVENTSITE
    where dataareaid not in ('044', '045', '047', '999')
    group by src_sys_nm, src_site_id, _fivetran_deleted 
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_site_id as 
    varchar
), '') as 
    varchar
)) as opco_site_sk,
    md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_site_id as 
    varchar
), '') || '-' || coalesce(cast(src_site_nm as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_site_ck, * 
    from ax_opco_site
   
)


select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site ) 
    
