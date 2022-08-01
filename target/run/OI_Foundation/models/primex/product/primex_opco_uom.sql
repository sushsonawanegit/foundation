

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_uom  as
      (




with opco_uom as(
  
    select 
    current_timestamp::timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'SYSPRO-PRMX'::varchar as src_sys_nm,
    'Amount'::varchar as src_uom_cd,
    '$'::varchar as src_uom_desc
     union all 
  
    select 
    current_timestamp::timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'SYSPRO-PRMX'::varchar as src_sys_nm,
    'Hours'::varchar as src_uom_cd,
    'HRS'::varchar as src_uom_desc
    
  
),
final as(
    select md5(cast(coalesce(cast(src_sys_nm as 
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
    from opco_uom
)

select * from final
      );
    