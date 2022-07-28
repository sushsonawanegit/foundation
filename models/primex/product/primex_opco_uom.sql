{{
    config(
        materialized='table',
        transient=false
    )
}}

{% set values = ['$','HRS'] %}
{% set num = ['Amount','Hours'] %}

with opco_uom as(
  {% for i in range(2) %}
    select 
    current_timestamp::timestamp as crt_dtm,
    null::timestamp_tz as stg_load_dtm,
    null::timestamp_tz as delete_dtm,
    'SYSPRO-PRMX'::varchar as src_sys_nm,
    '{{num[i]}}'::varchar as src_uom_cd,
    '{{values[i]}}'::varchar as src_uom_desc
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd']) }} as opco_lob_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_uom_cd', 'src_uom_desc']) }} as opco_lob_ck,
        *
    from opco_uom
)

select * from final