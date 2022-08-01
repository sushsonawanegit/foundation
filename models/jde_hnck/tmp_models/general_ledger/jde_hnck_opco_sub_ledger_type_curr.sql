with jde_hnck_opco_sub_ledger_type as(
    select *
    from {{ ref('jde_hnck_opco_sub_ledger_type') }}
)

select * from jde_hnck_opco_sub_ledger_type