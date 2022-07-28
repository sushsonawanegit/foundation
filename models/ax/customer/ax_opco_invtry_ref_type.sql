{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_invtry_ref_type as (
  {% set values = ['Sales Order', 'Purchase Order', 'Production', 'Production Line', 'Inventory Journal', 'CRM Quotation', 'Transfer Order', 'Fixed Asset'] %}

  {% for i in range(9) %}
    select 
        current_timestamp as crt_dtm,
        'AX' as src_sys_nm,
        {{i}} as invtry_ref_type_cd,
        {% if i == 0 %}
        null as invtry_ref_type_desc
        {% else %}
        '{{values[i-1]}}' as invtry_ref_type_desc
        {% endif %}
        {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'invtry_ref_type_cd']) }} as opco_invtry_ref_type_sk, 
            {{dbt_utils.surrogate_key(['src_sys_nm', 'invtry_ref_type_cd', 'invtry_ref_type_desc'])}} as opco_invtry_ref_type_ck, 
            *
    from opco_invtry_ref_type
)
select * from final