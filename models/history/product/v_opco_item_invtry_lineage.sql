with v_opco_item_invtry_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_invtry')])}}
)

select * from v_opco_item_invtry_lineage