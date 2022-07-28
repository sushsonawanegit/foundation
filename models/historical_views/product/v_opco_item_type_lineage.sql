with v_opco_item_type_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_type')])}}
)

select * from v_opco_item_type_lineage