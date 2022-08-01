with v_opco_lob as(
    {{ union_relations(
        relations=[ref('ax_opco_lob'), ref('syspro_primex_opco_lob')])}}
)

select * from v_opco_lob