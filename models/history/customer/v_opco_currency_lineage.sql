with v_opco_currency as(
    {{ union_relations(
        relations=[ref('ax_opco_currency'), ref('ns_opco_currency'), ref('jde_hnck_opco_currency'), ref('syspro_primex_opco_currency')])}}
)

select * from v_opco_currency