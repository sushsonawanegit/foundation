

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_invtry_ref_type  as
      (

with opco_invtry_ref_type as (
  

  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        0 as invtry_ref_type_cd,
        
        null as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        1 as invtry_ref_type_cd,
        
        'Sales Order' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        2 as invtry_ref_type_cd,
        
        'Purchase Order' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        3 as invtry_ref_type_cd,
        
        'Production' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        4 as invtry_ref_type_cd,
        
        'Production Line' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        5 as invtry_ref_type_cd,
        
        'Inventory Journal' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        6 as invtry_ref_type_cd,
        
        'CRM Quotation' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        7 as invtry_ref_type_cd,
        
        'Transfer Order' as invtry_ref_type_desc
        
         union all 
  
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        8 as invtry_ref_type_cd,
        
        'Fixed Asset' as invtry_ref_type_desc
        
        
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_invtry_ref_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_type_cd as 
    varchar
), '') || '-' || coalesce(cast(invtry_ref_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_invtry_ref_type_ck, 
            *
    from opco_invtry_ref_type
)
select * from final
      );
    