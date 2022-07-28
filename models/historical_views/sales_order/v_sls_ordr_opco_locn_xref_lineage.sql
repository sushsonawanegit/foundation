with v_sls_ordr_opco_locn_xref_lineage as(
    {{ union_relations(
        relations=[ref('ax_sls_ordr_opco_locn_xref')])}}
)

select * from v_sls_ordr_opco_locn_xref_lineage