{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_handling_status as (
  {% set values = ['None', 'Registered', 'Reserved', 'Activated', 'Started', 'Picked', 'Staged', 'Loaded', 'Completed', 'Canceled'] %}
  {% set num = [0, 1, 2, 3, 4, 7, 8, 9, 10, 20] %}

  {% for i in range(10) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{num[i]}} as handling_status_cd,
          '{{values[i]}}' as handling_status_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'handling_status_cd']) }} as opco_handling_status_sk,
            {{dbt_utils.surrogate_key(['src_sys_nm', 'handling_status_cd', 'handling_status_desc'])}} as opco_handling_status_ck, 
            *
    from opco_handling_status
)

select * from final