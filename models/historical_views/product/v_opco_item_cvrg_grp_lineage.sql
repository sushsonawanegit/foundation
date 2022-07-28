with v_opco_item_cvrg_grp_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_cvrg_grp')])}}
)

select * from v_opco_item_cvrg_grp_lineage