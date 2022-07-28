with opco_po_type as(
    select *
    from {{ ref('v_opco_po_type_lineage') }}

)

select * from opco_po_type

