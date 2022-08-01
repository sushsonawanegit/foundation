with v_opco_type as(
    {{ union_relations(
        relations=[ref('ax_opco_type'), ref('ns_opco_type'), ref('syspro_primex_opco_type')])}}
)

select * from v_opco_type