{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_trans_status as (
  {% set values = ['Open Order', 'Delivered', 'Invoiced', 'Canceled'] %}

  {% for i in range(5) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{ i }} as src_trans_status_cd,
          {% if i == 0 %}
            null as src_trans_status_desc
          {% else %}
            '{{values[i-1]}}' as src_trans_status_desc
          {% endif %}
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select 
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_trans_status_cd']) }} as opco_trans_status_sk, 
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_trans_status_cd','src_trans_status_desc']) }} as opco_trans_status_ck, 
      *
    from opco_trans_status
)

select * from final