with opco_invtry_issue_status as(
    select *
    from {{ ref('v_opco_invtry_issue_status_lineage') }}
)

select * from opco_invtry_issue_status