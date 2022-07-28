with v_opco_pymnt_terms_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_pymnt_terms'),ref('ns_opco_pymnt_terms')])}}
)

select * from v_opco_pymnt_terms_lineage