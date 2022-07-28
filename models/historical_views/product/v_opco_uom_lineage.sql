with v_opco_uom as(
    {{ union_relations(
        relations=[ref('ax_opco_uom'), ref('ns_opco_uom'), ref('jde_opco_uom'), ref('primex_opco_uom')])}}
)

select * from v_opco_uom