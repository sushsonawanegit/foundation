

with sub_region as(
    SELECT 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    subregion_opi_id::varchar(15) as sub_region_id,
    subregion_opi_name::varchar(100) as sub_region_nm,
    case 
        when is_inactive = 'F' then 1
        when is_inactive = 'T' then 0
    end::numeric(1,0) as actv_ind
    from FIVETRAN.NETSUITE.SUBREGION_OPI   
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(sub_region_id as 
    varchar
), '') as 
    varchar
)) as sub_region_sk,
     md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(sub_region_id as 
    varchar
), '') || '-' || coalesce(cast(sub_region_nm as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as sub_region_ck,* 
    from sub_region 
)


select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_sub_region ) 
    
