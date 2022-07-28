with v_opco_project_trans_origin_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_trans_origin')])}}
)

select * from v_opco_project_trans_origin_lineage