with syspro_primex_opco_chart_of_accts_type as(
    select *
    from {{ ref('syspro_primex_opco_chart_of_accts_type') }}
)

select * from syspro_primex_opco_chart_of_accts_type