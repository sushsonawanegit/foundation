with v_opco_item_size_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_size')])}}
)

select * from v_opco_item_size_lineage