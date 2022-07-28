{{
    config(
        materialized='table',
        transient=false
    )
}}

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
    select  {{ dbt_utils.surrogate_key(["'AX'", 'src_acct_type_cd']) }} as opco_chart_of_accts_type_sk, 
            {{ dbt_utils.surrogate_key(["'AX'", 'src_acct_type_cd', 'src_acct_type_desc']) }} as opco_chart_of_accts_type_ck, 
            current_timestamp as crt_dtm, 'AX'::varchar(20) as src_sys_nm, * 
    from opco_chart_of_accts_type  
)

select * from final