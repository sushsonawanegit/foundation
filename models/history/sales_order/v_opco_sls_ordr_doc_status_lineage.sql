with v_opco_sls_ordr_doc_status_lineage as(
    {{ union_relations(
        relations=[ref('ax_opco_sls_ordr_doc_status')])}}
)

select * from v_opco_sls_ordr_doc_status_lineage