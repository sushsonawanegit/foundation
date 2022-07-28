with v_opco_po_line_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_po_line')])}}
)

select * from v_opco_po_line_lineage