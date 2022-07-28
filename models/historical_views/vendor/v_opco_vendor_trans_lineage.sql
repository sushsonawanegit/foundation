with v_opco_vendor_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_vendor_trans')])}}
)

select * from v_opco_vendor_trans_lineage 