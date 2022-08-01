with v_opco_invtry_trans_type_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_invtry_trans_type')])}}
)

select * from v_opco_invtry_trans_type_lineage