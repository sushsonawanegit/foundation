{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_sls_ordr_type as (
  {% set values = ['Journal', 'Quotation', 'Subscription', 'Sales Order', 'Returned Order', 'Blanket Order', 'Item Requirements'] %}

  {% for i in range(7) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{ i }} as src_sls_ordr_type_cd,
          '{{values[i]}}' as src_sls_ordr_doc_status_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sls_ordr_type_cd']) }} as opco_sls_ordr_type_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sls_ordr_type_cd','src_sls_ordr_doc_status_desc']) }} as opco_sls_ordr_type_ck, 
        *
    from opco_sls_ordr_type 
)

select * from final