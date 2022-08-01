with v_opco_pymnt_mode_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_pymnt_mode'),ref('ns_opco_pymnt_mode')])}}
)

select * from v_opco_pymnt_mode_lineage