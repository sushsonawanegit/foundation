

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_type  as
      (

with opco_po_type as (
  
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as src_po_type_cd,
          'Journal' as src_po_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as src_po_type_cd,
          'Quotation' as src_po_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as src_po_type_cd,
          'Subscription' as src_po_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as src_po_type_cd,
          'Purchase Order' as src_po_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as src_po_type_cd,
          'Returned Order' as src_po_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as src_po_type_cd,
          'Blanket Order' as src_po_type_desc
          
  
),
final as(
  select 
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_po_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_po_type_sk,
      md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_po_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_po_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_po_type_ck, 
      *
  from opco_po_type
)

select * from final
      );
    