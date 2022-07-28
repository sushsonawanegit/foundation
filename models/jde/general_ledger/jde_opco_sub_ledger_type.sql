{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_sub_ledger_type as(
    {% set values = [ 'Address Book', 'Cost Center', 'Equipment', 'Case Number', 'Service Contract', 'Item Number', 'Lease Number', 'Order Number', 'Unit Number', 'Free Form, Alpha, left Justified', 'Free Form, Numeric, zero fill', 'Free Form, Alpha, blank fill' ] %}
    {% set num = [ 'A', 'C', 'E', 'F', 'G', 'I', 'L', 'O', 'U', 'W', 'Y', 'Z' ] %}

  {% for i in range(12) %}
    select current_timestamp as crt_dtm,
          'JDE-HNCK'::varchar(20) as src_sys_nm,
          '{{num[i]}}'::varchar(20) as src_sub_ledger_type_cd,
          '{{values[i]}}'::varchar(50) as src_sub_ledger_type_desc,
          '1'::number(1,0) as actv_ind
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select  {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd']) }} as opco_sub_ledger_type_sk,
            {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd', 'src_sub_ledger_type_desc', 'actv_ind']) }} as opco_sub_ledger_type_ck,
            *
    from opco_sub_ledger_type
)

select * from final