with v_opco_project_catgry_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_catgry')])}}
)

select * from v_opco_project_catgry_lineage