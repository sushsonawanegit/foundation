

with opco_sls_ordr_doc_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as src_sls_ordr_doc_status_cd,
          'None' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as src_sls_ordr_doc_status_cd,
          'Quotation' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as src_sls_ordr_doc_status_cd,
          'Purchase Order' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as src_sls_ordr_doc_status_cd,
          'Confirmation' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as src_sls_ordr_doc_status_cd,
          'Picking List' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as src_sls_ordr_doc_status_cd,
          'Packing List' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as src_sls_ordr_doc_status_cd,
          'Receipts List' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as src_sls_ordr_doc_status_cd,
          'Invoice' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as src_sls_ordr_doc_status_cd,
          'Invoice Approval Journal' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as src_sls_ordr_doc_status_cd,
          'Project - Invoice' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as src_sls_ordr_doc_status_cd,
          'Project - Packing Slip' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          11 as src_sls_ordr_doc_status_cd,
          'CRM Quotation' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          12 as src_sls_ordr_doc_status_cd,
          'Lost' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          13 as src_sls_ordr_doc_status_cd,
          'Canceled' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          14 as src_sls_ordr_doc_status_cd,
          'Free Text Invoice' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          15 as src_sls_ordr_doc_status_cd,
          'Request For Quote' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          16 as src_sls_ordr_doc_status_cd,
          'Request For Quote - Accept' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          17 as src_sls_ordr_doc_status_cd,
          'Request For Quote - Reject' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          18 as src_sls_ordr_doc_status_cd,
          'Purchase Requisition' as src_sls_ordr_doc_status_desc
          
  
),
final as(
    select 
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_doc_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_doc_status_sk,
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_doc_status_cd as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_doc_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_doc_status_ck, 
      *
    from opco_sls_ordr_doc_status
)

select * from final