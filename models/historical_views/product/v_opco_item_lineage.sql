with v_opco_item_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item')])}}
)

select * from v_opco_item_lineage