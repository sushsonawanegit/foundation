with v_opco_sls_ordr_type_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_sls_ordr_type')])}}
)

select * from v_opco_sls_ordr_type_lineage