with ax_opco_invtry_ref_type as(
    select *
    from {{ ref('ax_opco_invtry_ref_type') }}

)

select * from ax_opco_invtry_ref_type

