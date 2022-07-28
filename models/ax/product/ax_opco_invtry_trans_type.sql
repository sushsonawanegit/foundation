{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_invtry_trans_type as (
  {% set values = [ 'Sales Order', 'Production', 'Purchase Order', 'Transaction', 'Profit/Loss', 'Transfer', 'Weighted Average Inventory Closing', 'Production Line', 'BOM Line', 'BOM', 'Output Order', 'Project', 'Counting', 'Pallet Transport', 'Quarantine Order', 'Outdated', 'Fixed Assets', 'Transfer Order - Shipment', 'Tranfer Order - Receive', 'Transfer Order - Scrap', 'Quotation', 'Quality Order' ] %}
  {% set num = [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 21, 22, 23, 24, 25] %}

  {% for i in range(22) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{num[i]}} as invtry_trans_type_cd,
          '{{values[i]}}' as invtry_trans_type_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_trans_type_cd']) }} as opco_invtry_trans_type_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_trans_type_cd', 'invtry_trans_type_desc']) }} as opco_invtry_trans_type_ck,
            *
    from opco_invtry_trans_type
)

select * from final