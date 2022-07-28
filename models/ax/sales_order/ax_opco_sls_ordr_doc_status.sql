{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_sls_ordr_doc_status as (
  {% set values = ['None', 'Quotation', 'Purchase Order', 'Confirmation', 'Picking List', 'Packing List', 'Receipts List', 'Invoice', 'Invoice Approval Journal', 'Project - Invoice', 'Project - Packing Slip', 'CRM Quotation', 'Lost', 'Canceled', 'Free Text Invoice', 'Request For Quote', 'Request For Quote - Accept', 'Request For Quote - Reject', 'Purchase Requisition'] %}

  {% for i in range(19) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{ i }} as src_sls_ordr_doc_status_cd,
          '{{values[i]}}' as src_sls_ordr_doc_status_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select 
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sls_ordr_doc_status_cd']) }} as opco_sls_ordr_doc_status_sk,
      {{ dbt_utils.surrogate_key(['src_sys_nm', 'src_sls_ordr_doc_status_cd','src_sls_ordr_doc_status_desc']) }} as opco_sls_ordr_doc_status_ck, 
      *
    from opco_sls_ordr_doc_status
)

select * from final
