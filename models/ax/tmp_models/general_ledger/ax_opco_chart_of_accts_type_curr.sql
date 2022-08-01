with ax_opco_chart_of_accts_type as(
    select *
    from {{ ref('ax_opco_chart_of_accts_type') }}
)

select * from ax_opco_chart_of_accts_type