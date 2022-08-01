with v_opco as(
    {{ union_relations(
        relations=[ref('ax_opco'), ref('ns_opco'), ref('jde_opco'), ref('primex_opco')])}}
)

select * from v_opco