with v_opco_item_freight_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_freight')])}}
)

select * from v_opco_item_freight_lineage