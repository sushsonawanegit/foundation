

with opco_trans_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as src_trans_status_cd,
          
            null as src_trans_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as src_trans_status_cd,
          
            'Open Order' as src_trans_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as src_trans_status_cd,
          
            'Delivered' as src_trans_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as src_trans_status_cd,
          
            'Invoiced' as src_trans_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as src_trans_status_cd,
          
            'Canceled' as src_trans_status_desc
          
          
  
),
final as(
    select 
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_trans_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_trans_status_sk, 
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_trans_status_cd as 
    varchar
), '') || '-' || coalesce(cast(src_trans_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_trans_status_ck, 
      *
    from opco_trans_status
)

select * from final