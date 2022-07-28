{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_invtry_issue_status as (
  {% set values = [ 'Sold', 'Deducted', 'Picked', 'Reserved Physical', 'Reserved Ordered', 'On Order', 'Quotation Issue' ] %}

  {% for i in range(8) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{i}} as invtry_issue_status_cd,
          {% if i == 0 %}
            null as invtry_issue_status_desc
          {% else %}
            '{{values[i-1]}}' as invtry_issue_status_desc
          {% endif %}
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_issue_status_cd']) }} as opco_invtry_issue_status_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_issue_status_cd', 'invtry_issue_status_desc']) }} as opco_invtry_issue_status_ck,
            *
    from opco_invtry_issue_status
)

select * from final