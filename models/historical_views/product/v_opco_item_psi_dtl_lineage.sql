with v_opco_item_psi_dtl_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_item_psi_dtl')])}}
)

select * from v_opco_item_psi_dtl_lineage