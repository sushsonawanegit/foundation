{{
    config(
        materialized='table',
        transient=false
    )
}}

with jde_npp_opco_sub_ledger_type as(
    {% set codes = ['A', 'C', 'E', 'F', 'G', 'I', 'J', 'L', 'M', 'O', 'P', 'S', 'U', 'W', 'X', 'Y', 'Z'] %}
    {% set descs = ['Address Book Number', 'Business Unit Number', 'Equipment Number', 'Case Number', 'Service Contract Header', 'Item Number (Short)', 'Job Change Request', 'Lease Number', 'SUmmarized Work Order Number', 'Order Number', 'Revenue Performance Obligation', 'Structured Subledger', 'Unit Number', 'Work Order Number', 'Free Form, Alpha, left Justified', 'Free Form, Numeric, zero fill', 'Free Form, Alpha, blank fill'] %}

    {% for i in range(17) %}
        select 
        current_timestamp as crt_dtm,
        null::timestamp_tz as stg_load_dtm,
        null::timestamp_tz as delete_dtm,
        'JDE-NPP' as src_sys_nm,
        '{{codes[i]}}' as src_sub_ledger_type_cd,
        '{{descs[i]}}' as src_sub_ledger_type_desc,
        1::numeric(1,0) as actv_ind
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
),
final as (
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd'])}} as opco_sub_ledger_type_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sub_ledger_type_cd', 'src_sub_ledger_type_desc', 'actv_ind'])}} as opco_sub_ledger_type_ck,
        crt_dtm, stg_load_dtm, delete_dtm, src_sys_nm, src_sub_ledger_type_cd, src_sub_ledger_type_desc, actv_ind
        from jde_npp_opco_sub_ledger_type
)

select * from final