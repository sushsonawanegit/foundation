with ns_opco_gl_trans_type as(
    select *
    from {{ ref('ns_opco_gl_trans_type') }}
)

select * from ns_opco_gl_trans_type