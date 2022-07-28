with opco_sls_ordr_doc_status as(
    select *
    from {{ ref('v_opco_sls_ordr_doc_status_lineage') }}
)

select * from opco_sls_ordr_doc_status

