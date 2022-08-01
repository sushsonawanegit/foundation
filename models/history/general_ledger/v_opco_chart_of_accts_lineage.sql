with v_opco_chart_of_accts as(
    {{ union_relations(
        relations=[ref('ax_opco_chart_of_accts'), ref('ns_opco_chart_of_accts'), ref('jde_hnck_opco_chart_of_accts'), ref('syspro_primex_opco_chart_of_accts')])}}
)

select * from v_opco_chart_of_accts