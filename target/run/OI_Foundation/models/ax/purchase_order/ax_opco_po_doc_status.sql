

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_doc_status  as
      (

with opco_po_doc_status as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as po_doc_status_cd,
          'None' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as po_doc_status_cd,
          'Quotation' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as po_doc_status_cd,
          'Purchase Order' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as po_doc_status_cd,
          'Confirmation' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as po_doc_status_cd,
          'Picking List' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as po_doc_status_cd,
          'Packing Slip' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as po_doc_status_cd,
          'Receipts List' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as po_doc_status_cd,
          'Invoice' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as po_doc_status_cd,
          'Invoice Approval Journal' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as po_doc_status_cd,
          'Project - Invoice' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as po_doc_status_cd,
          'Project - Packing Slip' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          11 as po_doc_status_cd,
          'CRM Quotation' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          12 as po_doc_status_cd,
          'Lost' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          13 as po_doc_status_cd,
          'Canceled' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          14 as po_doc_status_cd,
          'Free Text Invoice' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          15 as po_doc_status_cd,
          'Request for Quote' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          16 as po_doc_status_cd,
          'Request for Quote - Accept' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          17 as po_doc_status_cd,
          'Request for Quote - Reject' as po_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          18 as po_doc_status_cd,
          'Purchase Requisition' as po_doc_status_desc
          
  
),
final as(
  select 
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(po_doc_status_cd as 
    varchar
), '') as 
    varchar
)) as opco_po_doc_status_sk,
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(po_doc_status_cd as 
    varchar
), '') || '-' || coalesce(cast(po_doc_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_po_doc_status_ck, 
      *
  from opco_po_doc_status
)

select * from final
      );
    