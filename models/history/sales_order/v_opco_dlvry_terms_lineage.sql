with v_opco_dlvry_terms_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_dlvry_terms')])}}
)

select * from v_opco_dlvry_terms_lineage