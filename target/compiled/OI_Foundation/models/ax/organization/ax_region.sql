

with ax_region as (
    SELECT DISTINCT
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'AX'::varchar(20) as src_sys_nm,
    upper(region_opi)::varchar(25) as region_id,
    region_opi::varchar(50) as region_nm,
    1::numeric(1,0) as actv_ind
    from FIVETRAN.AX_PRODREPLICATION1_DBO.COMPANYINFO
    where dataareaid not in ('044', '045', '047', '999')
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(region_id as 
    varchar
), '') as 
    varchar
)) as region_sk, 
           md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(region_id as 
    varchar
), '') || '-' || coalesce(cast(region_nm as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as region_ck,
           * 
    from ax_region
    
)
select * from final



    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_region ) 
    
