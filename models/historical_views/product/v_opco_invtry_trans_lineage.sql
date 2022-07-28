with v_opco_invtry_trans_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_invtry_trans')])}}
)

select * from v_opco_invtry_trans_lineage