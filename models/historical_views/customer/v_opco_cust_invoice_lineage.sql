with v_opco_cust_invoice_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_cust_invoice')])}}
)

select * from v_opco_cust_invoice_lineage