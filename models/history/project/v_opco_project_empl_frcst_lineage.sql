with v_opco_project_empl_frcst_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_empl_frcst')])}}
)

select * from v_opco_project_empl_frcst_lineage