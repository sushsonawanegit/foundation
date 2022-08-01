with ns_opco_chart_of_accts_type as(
    select *
    from {{ ref('ns_opco_chart_of_accts_type') }}
)

select * from ns_opco_chart_of_accts_type