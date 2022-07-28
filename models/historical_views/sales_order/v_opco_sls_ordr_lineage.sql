with v_opco_sls_ordr_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_sls_ordr')])}}
)

select * from v_opco_sls_ordr_lineage