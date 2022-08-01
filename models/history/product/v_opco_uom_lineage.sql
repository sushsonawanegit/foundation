with v_opco_uom as(
    {{ union_relations(
        relations=[ref('ax_opco_uom'), ref('ns_opco_uom'), ref('jde_hnck_opco_uom'), ref('syspro_primex_opco_uom')])}}
)

select * from v_opco_uom