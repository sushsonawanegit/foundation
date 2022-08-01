

with opco_gl_trans_type as (
  

  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(0 as string) as src_gl_trans_type_cd,
          
            null::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(1 as string) as src_gl_trans_type_cd,
          
            'Transfer'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(2 as string) as src_gl_trans_type_cd,
          
            'Sales Order'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(3 as string) as src_gl_trans_type_cd,
          
            'Purchase Order'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(4 as string) as src_gl_trans_type_cd,
          
            'Inventory'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(5 as string) as src_gl_trans_type_cd,
          
            'Production'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(6 as string) as src_gl_trans_type_cd,
          
            'Project'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(7 as string) as src_gl_trans_type_cd,
          
            'Interest'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(8 as string) as src_gl_trans_type_cd,
          
            'Customer'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(9 as string) as src_gl_trans_type_cd,
          
            'Exchange Adjustment'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(10 as string) as src_gl_trans_type_cd,
          
            'Totaled'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(11 as string) as src_gl_trans_type_cd,
          
            'Payroll'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(12 as string) as src_gl_trans_type_cd,
          
            'Fixed Assets'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(13 as string) as src_gl_trans_type_cd,
          
            'Collection Letter'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(14 as string) as src_gl_trans_type_cd,
          
            'Vendor'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(15 as string) as src_gl_trans_type_cd,
          
            'Payment'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(16 as string) as src_gl_trans_type_cd,
          
            'Sales Tax'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(17 as string) as src_gl_trans_type_cd,
          
            'Bank'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(18 as string) as src_gl_trans_type_cd,
          
            'Conversion'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(19 as string) as src_gl_trans_type_cd,
          
            'Bill Of Exchange'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(20 as string) as src_gl_trans_type_cd,
          
            'Promissory Note'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(21 as string) as src_gl_trans_type_cd,
          
            'Cost Accounting'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(22 as string) as src_gl_trans_type_cd,
          
            'Labor'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(23 as string) as src_gl_trans_type_cd,
          
            'Fee'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(24 as string) as src_gl_trans_type_cd,
          
            'Settlement'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(25 as string) as src_gl_trans_type_cd,
          
            'Allocation'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(26 as string) as src_gl_trans_type_cd,
          
            'Elimination'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(27 as string) as src_gl_trans_type_cd,
          
            'Cash Discount'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(28 as string) as src_gl_trans_type_cd,
          
            'Overpayment/Underpayment'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(29 as string) as src_gl_trans_type_cd,
          
            'Penny Difference'::varchar(100) as src_gl_trans_type_desc
          
           union all 
  
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast(30 as string) as src_gl_trans_type_cd,
          
            'Intercompany Settlement'::varchar(100) as src_gl_trans_type_desc
          
          
  
),
final as(
    select  md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_gl_trans_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_gl_trans_type_ck,
            *
    from opco_gl_trans_type
)

select * from final