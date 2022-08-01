with v_opco_dept as(
    {{ union_relations(
        relations=[ref('ax_opco_dept'), ref('ns_opco_dept'), ref('syspro_primex_opco_dept')])}}
)

select * from v_opco_dept