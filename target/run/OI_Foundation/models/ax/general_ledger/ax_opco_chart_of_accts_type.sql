

      create or replace  table OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts_type  as
      (

with opco_chart_of_accts_type as (
    select
    '0'::varchar(10) as src_acct_type_cd,
    'P&L'::varchar(100) as src_acct_type_desc
    union
    select
    '3'::varchar(10) as src_acct_type_cd,
    'Balance'::varchar(100) as src_acct_type_desc
    union
    select
    '6'::varchar(10) as src_acct_type_cd,
    'Header'::varchar(100) as src_acct_type_desc
    union
    select
    '9'::varchar(10) as src_acct_type_cd,
    'Total'::varchar(100) as src_acct_type_desc
    union
    select
    '100'::varchar(10) as src_acct_type_cd,
    'Statistical'::varchar(100) as src_acct_type_desc
),
final as(
    select  md5(cast(coalesce(cast('AX' as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_cd as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_sk, 
            md5(cast(coalesce(cast('AX' as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_cd as 
    varchar
), '') || '-' || coalesce(cast(src_acct_type_desc as 
    varchar
), '') as 
    varchar
)) as opco_chart_of_accts_type_ck, 
            current_timestamp as crt_dtm, 'AX'::varchar(20) as src_sys_nm, * 
    from opco_chart_of_accts_type  
)

select * from final
      );
    