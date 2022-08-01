

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_rcpt_status  as
      (

with opco_invtry_rcpt_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as invtry_rcpt_status_cd,
          
            null as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as invtry_rcpt_status_cd,
          
            'Purchased' as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as invtry_rcpt_status_cd,
          
            'Received' as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as invtry_rcpt_status_cd,
          
            'Registered' as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as invtry_rcpt_status_cd,
          
            'Arrived' as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as invtry_rcpt_status_cd,
          
            'Ordered' as invtry_rcpt_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as invtry_rcpt_status_cd,
          
            'Quotation Receipt' as invtry_rcpt_status_desc
          
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_rcpt_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_invtry_rcpt_status_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_rcpt_status_cd as 
    varchar
), '') || '-' || coalesce(cast(invtry_rcpt_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_invtry_rcpt_status_ck,
            *
    from opco_invtry_rcpt_status
)

select * from final
      );
    