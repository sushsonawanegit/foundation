with v_ax_opco_gl_trans_posting_type_curr as(
    select *
    from {{ ref('ax_opco_gl_trans_posting_type') }}
)

select * from v_ax_opco_gl_trans_posting_type_curr