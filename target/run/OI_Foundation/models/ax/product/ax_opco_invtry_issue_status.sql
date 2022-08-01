

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_issue_status  as
      (

with opco_invtry_issue_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as invtry_issue_status_cd,
          
            null as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as invtry_issue_status_cd,
          
            'Sold' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as invtry_issue_status_cd,
          
            'Deducted' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as invtry_issue_status_cd,
          
            'Picked' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as invtry_issue_status_cd,
          
            'Reserved Physical' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as invtry_issue_status_cd,
          
            'Reserved Ordered' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as invtry_issue_status_cd,
          
            'On Order' as invtry_issue_status_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as invtry_issue_status_cd,
          
            'Quotation Issue' as invtry_issue_status_desc
          
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_issue_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_invtry_issue_status_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_issue_status_cd as 
    varchar
), '') || '-' || coalesce(cast(invtry_issue_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_invtry_issue_status_ck,
            *
    from opco_invtry_issue_status
)

select * from final
      );
    