with v_opco_project_item_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_item_trans')])}}
)

select * from v_opco_project_item_trans_lineage 