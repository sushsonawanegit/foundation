{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_po_type as (
  {% set values = [ 'Journal', 'Quotation', 'Subscription', 'Purchase Order', 'Returned Order', 'Blanket Order'] %}
  {% set num = [0, 1, 2, 3, 4, 5] %}

  {% for i in range(6) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{num[i]}} as src_po_type_cd,
          '{{values[i]}}' as src_po_type_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
  select 
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_po_type_cd']) }} as opco_po_type_sk,
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_po_type_cd','src_po_type_desc']) }} as opco_po_type_ck, 
      *
  from opco_po_type
)

select * from final