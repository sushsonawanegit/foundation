

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_handling_status  as
      (

with opco_handling_status as (
  
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as handling_status_cd,
          'None' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as handling_status_cd,
          'Registered' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as handling_status_cd,
          'Reserved' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as handling_status_cd,
          'Activated' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as handling_status_cd,
          'Started' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as handling_status_cd,
          'Picked' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as handling_status_cd,
          'Staged' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as handling_status_cd,
          'Loaded' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as handling_status_cd,
          'Completed' as handling_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          20 as handling_status_cd,
          'Canceled' as handling_status_desc
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(handling_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_handling_status_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(handling_status_cd as 
    varchar
), '') || '-' || coalesce(cast(handling_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_handling_status_ck, 
            *
    from opco_handling_status
)

select * from final
      );
    