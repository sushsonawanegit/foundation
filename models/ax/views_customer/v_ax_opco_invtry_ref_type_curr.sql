with v_ax_opco_invtry_ref_type_curr as(
    select *
    from {{ ref('ax_opco_invtry_ref_type') }}

)

select * from v_ax_opco_invtry_ref_type_curr
