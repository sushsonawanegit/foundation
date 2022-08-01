with v_opco_item_opco_assctn_xref_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_opco_assctn_xref')])}}
)

select * from v_opco_item_opco_assctn_xref_lineage