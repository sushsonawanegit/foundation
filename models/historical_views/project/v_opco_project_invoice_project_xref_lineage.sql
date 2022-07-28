with v_opco_project_invoice_project_xr_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_project_invoice_project_xref')])}}
)

select * from v_opco_project_invoice_project_xr_lineage