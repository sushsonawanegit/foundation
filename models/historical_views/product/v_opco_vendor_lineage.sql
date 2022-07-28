with v_opco_vendor_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_vendor')])}}
)

select * from v_opco_vendor_lineage