with jde_npp_opco_sub_ledger_type_curr as(
    select *
    from {{ ref('jde_npp_opco_sub_ledger_type') }}
)

select * from jde_npp_opco_sub_ledger_type_curr