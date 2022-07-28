with v_opco_item_class_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_class')])}}
)

select * from v_opco_item_class_lineage