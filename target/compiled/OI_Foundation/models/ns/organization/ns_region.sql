

with region as (
    SELECT 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    region_opi_id::varchar(15) as region_id,
    region_opi_name::varchar(50) as region_nm,
    case 
        when is_inactive = 'F' then 1
        when is_inactive = 'T' then 0
    end::numeric(1,0) as actv_ind
    from FIVETRAN.NETSUITE.REGION_OPI
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
)) as region_ck, * 
    from region  
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_region ) 
    
