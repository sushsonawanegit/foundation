with v_opco_project_trans_posting_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_trans_posting')])}}
)

select * from v_opco_project_trans_posting_lineage