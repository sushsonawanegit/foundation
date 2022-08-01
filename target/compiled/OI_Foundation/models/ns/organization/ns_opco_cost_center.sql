

with ns_opco_cost_center as (
    select 
    current_timestamp as crt_dtm,
    _fivetran_synced as stg_load_dtm,
    case
        when _fivetran_deleted = true then stg_load_dtm
        else null
    end as delete_dtm,
    'NS'::varchar(20) as src_sys_nm,
    location_id::varchar(20) as src_cost_center_cd,
    name::varchar(100) as src_cost_center_desc,
    
    
    trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    IFNULL(NULLIF(src_cost_center_desc, ''), 'NULL'), 
                                '[ ]+', ' '),
                            '[\]\[\\\/!@#$%\^*(){}|?~`,\n\t\r]', ' '), 
                        '&', ' and '), 
                    '<', '.LT.'), 
                '>', '.GT.'), 
            '[\+]', ' plus '), 
        '=', ' equal '))
    
::varchar(100) as cost_center_wo_spcl_chr_cd,
    case
        when isinactive = 'Yes' then 0
        else 1
    end::numeric(1,0) as actv_ind,
    null::varchar(50) as src_cost_center_type_txt
    from FIVETRAN.NETSUITE.LOCATIONS  
),
final as (
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_cd as 
    varchar
), '') as 
    varchar
)) as opco_cost_center_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_cd as 
    varchar
), '') || '-' || coalesce(cast(src_cost_center_desc as 
    varchar
), '') as 
    varchar
)) as opco_cost_center_ck,
            *
    from ns_opco_cost_center 
)

select * from final




    
        where stg_load_dtm > (select max(stg_load_dtm) from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_cost_center ) 
    
