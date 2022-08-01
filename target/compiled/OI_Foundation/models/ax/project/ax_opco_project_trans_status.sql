

with opco_project_trans_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as project_trans_status_cd,
          'No Status' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as project_trans_status_cd,
          'Registered' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as project_trans_status_cd,
          'Posted' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as project_trans_status_cd,
          'Invoice Proposal' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as project_trans_status_cd,
          'Invoiced' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as project_trans_status_cd,
          'Selected For Credit Note' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as project_trans_status_cd,
          'Credit Note Proposal' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as project_trans_status_cd,
          'Estimated' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as project_trans_status_cd,
          'Eliminated' as project_trans_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as project_trans_status_cd,
          'Adjusted' as project_trans_status_desc
          
  
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(project_trans_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_project_trans_status_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(project_trans_status_cd as 
    varchar
), '') || '-' || coalesce(cast(project_trans_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_project_trans_status_ck,
        *
    from opco_project_trans_status
)

select * from final