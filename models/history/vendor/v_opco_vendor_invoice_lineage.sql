with v_opco_vendor_invoice_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_vendor_invoice')])}}
)

select * from v_opco_vendor_invoice_lineage