with v_opco_cost_center as(
    {{ union_relations(
        relations=[ref('ax_opco_cost_center'), ref('ns_opco_cost_center'), ref('jde_hnck_opco_cost_center'), ref('syspro_primex_opco_cost_center')])}}
)

select * from v_opco_cost_center