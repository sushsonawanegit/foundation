with v_opco_project_trans_status_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_trans_status')])}}
)

select * from v_opco_project_trans_status_lineage  