with opco_gl_trans_posting_type as(
    select *
    from {{ ref('v_opco_gl_trans_posting_type_lineage') }}
)

select * from opco_gl_trans_posting_type