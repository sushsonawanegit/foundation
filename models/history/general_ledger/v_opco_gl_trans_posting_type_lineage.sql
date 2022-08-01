with v_opco_gl_trans_posting_type as(
    {{ union_relations(
        relations=[ref('ax_opco_gl_trans_posting_type')])}}
)

select * from v_opco_gl_trans_posting_type