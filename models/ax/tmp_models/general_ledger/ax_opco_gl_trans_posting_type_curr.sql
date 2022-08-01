with ax_opco_gl_trans_posting_type as(
    select *
    from {{ ref('ax_opco_gl_trans_posting_type') }}
)

select * from ax_opco_gl_trans_posting_type