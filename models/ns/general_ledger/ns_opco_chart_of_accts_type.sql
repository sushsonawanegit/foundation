{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_chart_of_accts_type as (
  {% set values_cd = [ 'ACCOUNTS PAYABLE', 'ACCOUNTS RECEIVABLE', 'BANK', 'COST OF GOODS SOLD', 'CREDIT CARD', 'DEFERRED EXPENSE',
   'DEFERRED REVENUE', 'EQUITY', 'EXPENSE', 'FIXED ASSET', 'INCOME', 'LONG TERM LIABILITY', 'NON POSTING', 'OTHER ASSET',
    'OTHER CURRENT ASSET', 'OTHER CURRENT LIABILITY', 'OTHER EXPENSE', 'OTHER INCOME', 'STATISTICAL','UNBILLED RECEIVABLE'] %}


  {% set values_desc = [ 'Accounts Payable', 'Accounts Receivable', 'Bank', 'Cost Of Goods Sold', 'Credit Card', 'Deferred Expense',
   'Deferred Revenue', 'Equity', 'Expense', 'Fixed Asset', 'Income', 'Long Term Liability', 'Non-Posting', 'Other Asset',
    'Other Current Asset', 'Other Current Liability', 'Other Expense', 'Other Income', 'Statistical','Unbilled Receivable'] %}

  {% for i in range(20) %}
    select current_timestamp as crt_dtm,
        'NS'::varchar(20) as src_sys_nm,
        '{{values_cd[i]}}'::varchar(50) as src_acct_type_cd,
        '{{values_desc[i]}}'::varchar(100) as src_acct_type_desc
        {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_acct_type_cd']) }} as opco_chart_of_accts_type_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_acct_type_cd', 'src_acct_type_desc']) }} as opco_chart_of_accts_type_ck,
            * 
    from opco_chart_of_accts_type  
)

select * from final