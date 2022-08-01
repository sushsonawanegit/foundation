

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_trans_type  as
      (

with opco_invtry_trans_type as (
  
  

  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          0 as invtry_trans_type_cd,
          'Sales Order' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          2 as invtry_trans_type_cd,
          'Production' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          3 as invtry_trans_type_cd,
          'Purchase Order' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          4 as invtry_trans_type_cd,
          'Transaction' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          5 as invtry_trans_type_cd,
          'Profit/Loss' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          6 as invtry_trans_type_cd,
          'Transfer' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          7 as invtry_trans_type_cd,
          'Weighted Average Inventory Closing' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          8 as invtry_trans_type_cd,
          'Production Line' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          9 as invtry_trans_type_cd,
          'BOM Line' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          10 as invtry_trans_type_cd,
          'BOM' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          11 as invtry_trans_type_cd,
          'Output Order' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          12 as invtry_trans_type_cd,
          'Project' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          13 as invtry_trans_type_cd,
          'Counting' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          14 as invtry_trans_type_cd,
          'Pallet Transport' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          15 as invtry_trans_type_cd,
          'Quarantine Order' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          16 as invtry_trans_type_cd,
          'Outdated' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          20 as invtry_trans_type_cd,
          'Fixed Assets' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          21 as invtry_trans_type_cd,
          'Transfer Order - Shipment' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          22 as invtry_trans_type_cd,
          'Tranfer Order - Receive' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          23 as invtry_trans_type_cd,
          'Transfer Order - Scrap' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          24 as invtry_trans_type_cd,
          'Quotation' as invtry_trans_type_desc
           union all 
  
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          25 as invtry_trans_type_cd,
          'Quality Order' as invtry_trans_type_desc
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_invtry_trans_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_type_cd as 
    varchar
), '') || '-' || coalesce(cast(invtry_trans_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_invtry_trans_type_ck,
            *
    from opco_invtry_trans_type
)

select * from final
      );
    