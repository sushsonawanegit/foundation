with opco_invtry_ref_type as(
    select *
    from {{ ref('v_opco_invtry_ref_type_lineage') }}

)

select * from opco_invtry_ref_type

