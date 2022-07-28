with opco_gl_trans_type as(
    select *
    from {{ ref('v_opco_gl_trans_type_lineage') }}
)

select * from opco_gl_trans_type