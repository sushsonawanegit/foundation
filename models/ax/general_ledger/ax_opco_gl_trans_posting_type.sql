{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_gl_trans_type as (
  {% set values = [ 'Transfer', 'Sales Order', 'Purchase Order', 'Inventory', 'Production', 'Project', 'Interest', 'Customer', 'Exchange Adjustment', 'Totaled', 'Payroll', 'Fixed Assets', 'Collection Letter', 'Vendor', 'Payment', 'Sales Tax', 'Bank', 'Conversion', 'Bill Of Exchange', 'Promissory Note', 'Cost Accounting', 'Labor', 'Fee', 'Settlement', 'Allocation', 'Elimination', 'Cash Discount', 'Overpayment/Underpayment', 'Penny Difference', 'Intercompany Settlement'] %}

  {% for i in range(31) %}
    select current_timestamp as crt_dtm,
          'AX'::varchar(20) as src_sys_nm,
          cast({{ i }} as string) as src_gl_trans_type_cd,
          {% if i == 0 %}
            null::varchar(100) as src_gl_trans_type_desc
          {% else %}
            '{{values[i-1]}}'::varchar(100) as src_gl_trans_type_desc
          {% endif %}
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd']) }} as opco_gl_trans_type_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_gl_trans_type_cd', 'src_gl_trans_type_desc']) }} as opco_gl_trans_type_ck,
            *
    from opco_gl_trans_type
)

select * from final