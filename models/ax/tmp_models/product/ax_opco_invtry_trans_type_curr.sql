with ax_opco_invtry_trans_type as(
    select *
    from {{ ref('ax_opco_invtry_trans_type') }}
)

select * from ax_opco_invtry_trans_type