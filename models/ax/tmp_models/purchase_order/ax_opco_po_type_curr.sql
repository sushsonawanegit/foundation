with ax_opco_po_type as(
    select *
    from {{ ref('ax_opco_po_type') }}

)

select * from ax_opco_po_type

