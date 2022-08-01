with v_opco_purpose as(
    {{ union_relations(
        relations=[ref('ax_opco_purpose'), ref('ns_opco_purpose'), ref('syspro_primex_opco_purpose')])}}
)

select * from v_opco_purpose