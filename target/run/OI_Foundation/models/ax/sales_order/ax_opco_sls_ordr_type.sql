

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_type  as
      (

with opco_sls_ordr_type as (
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as src_sls_ordr_type_cd,
          'Journal' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          1 as src_sls_ordr_type_cd,
          'Quotation' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as src_sls_ordr_type_cd,
          'Subscription' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as src_sls_ordr_type_cd,
          'Sales Order' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as src_sls_ordr_type_cd,
          'Returned Order' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as src_sls_ordr_type_cd,
          'Blanket Order' as src_sls_ordr_doc_status_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as src_sls_ordr_type_cd,
          'Item Requirements' as src_sls_ordr_doc_status_desc
          
  
),
final as(
    select 
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_type_sk,
        md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_sls_ordr_doc_status_desc as 
    varchar
), '') as 
    varchar
)) as opco_sls_ordr_type_ck, 
        *
    from opco_sls_ordr_type 
)

select * from final
      );
    