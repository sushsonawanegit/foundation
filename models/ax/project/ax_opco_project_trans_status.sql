{{
    config(
        materialized='table',
        transient=false
    )
}}

with opco_project_trans_status as (
  {% set values = ['No Status', 'Registered', 'Posted', 'Invoice Proposal', 'Invoiced', 'Selected For Credit Note', 'Credit Note Proposal', 'Estimated', 'Eliminated', 'Adjusted'] %}

  {% for i in range(10) %}
    select current_timestamp as crt_dtm,
          'AX' as src_sys_nm,
          {{i}} as project_trans_status_cd,
          '{{values[i]}}' as project_trans_status_desc
          {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
final as(
    select 
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'project_trans_status_cd']) }} as opco_project_trans_status_sk,
        {{ dbt_utils.surrogate_key(['src_sys_nm', 'project_trans_status_cd','project_trans_status_desc']) }} as opco_project_trans_status_ck,
        *
    from opco_project_trans_status
)

select * from final