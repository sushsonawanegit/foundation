with opco_invtry_trans_type as(
    select *
    from {{ ref('v_opco_invtry_trans_type_lineage') }}
)

select * from opco_invtry_trans_type