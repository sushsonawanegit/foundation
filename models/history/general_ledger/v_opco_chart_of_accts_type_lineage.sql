with v_opco_chart_of_accts_type as(
    {{ union_relations(
        relations=[ref('ax_opco_chart_of_accts_type'), ref('ns_opco_chart_of_accts_type'), ref('syspro_primex_opco_chart_of_accts_type')])}}
)

select * from v_opco_chart_of_accts_type