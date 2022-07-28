{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_invtry_rcpt_status as (
  {% set values = [ 'Purchased', 'Received', 'Registered', 'Arrived', 'Ordered', 'Quotation Receipt' ] %}

  {% for i in range(7) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{i}} as invtry_rcpt_status_cd,
          {% if i == 0 %}
            null as invtry_rcpt_status_desc
          {% else %}
            '{{values[i-1]}}' as invtry_rcpt_status_desc
          {% endif %}
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_rcpt_status_cd']) }} as opco_invtry_rcpt_status_sk, 
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_rcpt_status_cd', 'invtry_rcpt_status_desc']) }} as opco_invtry_rcpt_status_ck,
            *
    from opco_invtry_rcpt_status
)

select * from final