with v_opco_vendor_packing_slip_line_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_vendor_packing_slip_line')])}}
)

select * from v_opco_vendor_packing_slip_line_lineage 