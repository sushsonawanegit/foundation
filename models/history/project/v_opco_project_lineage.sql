with v_opco_project_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project')])}}
)

select * from v_opco_project_lineage