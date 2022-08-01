with v_opco_po_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_po')])}}
)

select * from v_opco_po_lineage