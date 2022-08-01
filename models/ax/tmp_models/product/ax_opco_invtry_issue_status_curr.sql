with ax_opco_invtry_issue_status as(
    select *
    from {{ ref('ax_opco_invtry_issue_status') }}
)

select * from ax_opco_invtry_issue_status