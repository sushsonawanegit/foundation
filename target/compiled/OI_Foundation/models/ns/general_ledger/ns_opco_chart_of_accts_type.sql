

with opco_chart_of_accts_type as (
  


  

  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'ACCOUNTS PAYABLE'::varchar(50) as src_acct_type_cd,
        'Accounts Payable'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'ACCOUNTS RECEIVABLE'::varchar(50) as src_acct_type_cd,
        'Accounts Receivable'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'BANK'::varchar(50) as src_acct_type_cd,
        'Bank'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'COST OF GOODS SOLD'::varchar(50) as src_acct_type_cd,
        'Cost Of Goods Sold'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'CREDIT CARD'::varchar(50) as src_acct_type_cd,
        'Credit Card'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'DEFERRED EXPENSE'::varchar(50) as src_acct_type_cd,
        'Deferred Expense'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'DEFERRED REVENUE'::varchar(50) as src_acct_type_cd,
        'Deferred Revenue'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'EQUITY'::varchar(50) as src_acct_type_cd,
        'Equity'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'EXPENSE'::varchar(50) as src_acct_type_cd,
        'Expense'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'FIXED ASSET'::varchar(50) as src_acct_type_cd,
        'Fixed Asset'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'INCOME'::varchar(50) as src_acct_type_cd,
        'Income'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'LONG TERM LIABILITY'::varchar(50) as src_acct_type_cd,
        'Long Term Liability'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'NON POSTING'::varchar(50) as src_acct_type_cd,
        'Non-Posting'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'OTHER ASSET'::varchar(50) as src_acct_type_cd,
        'Other Asset'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'OTHER CURRENT ASSET'::varchar(50) as src_acct_type_cd,
        'Other Current Asset'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'OTHER CURRENT LIABILITY'::varchar(50) as src_acct_type_cd,
        'Other Current Liability'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'OTHER EXPENSE'::varchar(50) as src_acct_type_cd,
        'Other Expense'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'OTHER INCOME'::varchar(50) as src_acct_type_cd,
        'Other Income'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'STATISTICAL'::varchar(50) as src_acct_type_cd,
        'Statistical'::varchar(100) as src_acct_type_desc
         union all 
  
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        'UNBILLED RECEIVABLE'::varchar(50) as src_acct_type_cd,
        'Unbilled Receivable'::varchar(100) as src_acct_type_desc
        
  
),
final as(
    select  md5(cast(coalesce(cast(src_acct_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_sk, 
            md5(cast(coalesce(cast(src_sys_nm as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_ck,
            * 
    from opco_chart_of_accts_type  
)

select * from final