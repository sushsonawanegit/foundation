with v_opco_invtry_issue_status_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_invtry_issue_status')])}}
)

select * from v_opco_invtry_issue_status_lineage