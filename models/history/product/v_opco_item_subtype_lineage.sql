with v_opco_item_subtype_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_subtype')])}}
)

select * from v_opco_item_subtype_lineage