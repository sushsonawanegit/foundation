with v_opco_po_type_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_po_type')])}}
)

select * from v_opco_po_type_lineage