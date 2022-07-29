with v_ax_opco_chart_of_accts_type_curr as(
    select *
    from {{ ref('ax_opco_chart_of_accts_type') }}
)

select * from v_ax_opco_chart_of_accts_type_curr