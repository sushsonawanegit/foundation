with opco_chart_of_accts_type as(
    select *
    from {{ ref('v_opco_chart_of_accts_type_lineage') }}
)

select * from opco_chart_of_accts_type