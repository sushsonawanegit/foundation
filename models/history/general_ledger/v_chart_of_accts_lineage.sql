with v_chart_of_accts as(
    {{ union_relations(
        relations=[ref('ax_chart_of_accts')])}}
)

select * from v_chart_of_accts