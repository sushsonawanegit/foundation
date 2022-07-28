{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_project_trans_origin as (
  {% set values = ['None', 'Hour Journal', 'Expense Journal', 'General Journal', 'Invoice Journal', 'Invoice Approval Journal', 'Expense Management', 'Elimination Investment', 'Estimate Accrued Loss', 'Item Journal', 'Purchase Order', 'Item Requirement', 'Sales Order', 'Production-FInished', 'Production-Consumed', 'Fee Journal', 'Estimate Fee', 'Subscription', 'Prepayment', 'Deduction', 'Milestone', 'Invoice', 'Inventory Closing', 'Adjustment', 'Post Cost', 'Accrue Revenue', 'Post Estimate', 'Reverse Estimate', 'Eliminate Estimate', 'Reverse Elimination', 'Accrue Subscription Revenue'] %}
  {% set num = [0, 1, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59] %}

  {% for i in range(31) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{num[i]}} as project_trans_origin_cd,
          '{{values[i]}}' as project_trans_origin_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'project_trans_origin_cd']) }} as opco_project_trans_origin_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'project_trans_origin_cd', 'project_trans_origin_desc']) }} as opco_project_trans_origin_ck,
        *
    from opco_project_trans_origin
)

select * from final