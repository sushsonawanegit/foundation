

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_sub_ledger_type  as
      (

with opco_sub_ledger_type as(
    
    

  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'A'::varchar(20) as src_sub_ledger_type_cd,
          'Address Book'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'C'::varchar(20) as src_sub_ledger_type_cd,
          'Cost Center'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'E'::varchar(20) as src_sub_ledger_type_cd,
          'Equipment'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'F'::varchar(20) as src_sub_ledger_type_cd,
          'Case Number'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'G'::varchar(20) as src_sub_ledger_type_cd,
          'Service Contract'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'I'::varchar(20) as src_sub_ledger_type_cd,
          'Item Number'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'L'::varchar(20) as src_sub_ledger_type_cd,
          'Lease Number'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'O'::varchar(20) as src_sub_ledger_type_cd,
          'Order Number'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'U'::varchar(20) as src_sub_ledger_type_cd,
          'Unit Number'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'W'::varchar(20) as src_sub_ledger_type_cd,
          'Free Form, Alpha, left Justified'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'Y'::varchar(20) as src_sub_ledger_type_cd,
          'Free Form, Numeric, zero fill'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
           union all 
  
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          'Z'::varchar(20) as src_sub_ledger_type_cd,
          'Free Form, Alpha, blank fill'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_sub_ledger_type_sk,
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_sub_ledger_type_desc as 
    varchar
), '') || '-' || coalesce(cast(actv_ind as 
    varchar
), '') as 
    varchar
)) as opco_sub_ledger_type_ck,
            *
    from opco_sub_ledger_type
)

select * from final
      );
    