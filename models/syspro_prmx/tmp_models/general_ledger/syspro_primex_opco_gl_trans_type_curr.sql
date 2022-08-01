with syspro_primex_opco_gl_trans_type as(
    select *
    from {{ ref('syspro_primex_opco_gl_trans_type') }}
)

select * from syspro_primex_opco_gl_trans_type