with ax_opco_handling_status as(
    select *
    from {{ ref('ax_opco_handling_status') }}
)

select * from ax_opco_handling_status

