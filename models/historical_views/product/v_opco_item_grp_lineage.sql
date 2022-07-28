with v_opco_item_grp_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_grp')])}}
)

select * from v_opco_item_grp_lineage