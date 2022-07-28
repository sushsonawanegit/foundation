with opco_handling_status as(
    select *
    from {{ ref('v_opco_handling_status_lineage') }}
)

select * from opco_handling_status

