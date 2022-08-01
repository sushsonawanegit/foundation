with v_opco_dept as(
    {{ union_relations(
        relations=[ref('ax_opco_dept'), ref('ns_opco_dept'), ref('primex_opco_dept')])}}
)

select * from v_opco_dept