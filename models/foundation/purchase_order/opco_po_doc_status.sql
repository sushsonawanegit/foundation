with opco_po_doc_status as(
    select *
    from {{ ref('v_opco_po_doc_status_lineage') }}
)

select * from opco_po_doc_status

