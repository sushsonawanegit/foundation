with v_opco_gl_trans_type as(
    {{ union_relations(
        relations=[ref('ax_opco_gl_trans_type'), ref('ns_opco_gl_trans_type'), ref('jde_opco_gl_trans_type'), ref('primex_opco_gl_trans_type')])}}
)

select * from v_opco_gl_trans_type