with v_opco_gl_trans as(
    {{ union_relations(
        relations=[ref('ax_opco_gl_trans'), ref('ns_opco_gl_trans'), ref('jde_opco_gl_trans'), ref('primex_opco_gl_trans')])}}
)

select * from v_opco_gl_trans