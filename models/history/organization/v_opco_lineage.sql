with v_opco as(
    {{ union_relations(
        relations=[ref('ax_opco'), ref('ns_opco'), ref('jde_hnck_opco'), ref('syspro_primex_opco')])}}
)

select * from v_opco