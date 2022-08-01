with ax_opco_sls_ordr_doc_status as(
    select *
    from {{ ref('ax_opco_sls_ordr_doc_status') }}
)

select * from ax_opco_sls_ordr_doc_status

