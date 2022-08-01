with syspro_primex_opco_sub_ledger_type as(
    select *
    from {{ ref('syspro_primex_opco_sub_ledger_type') }}
)

select * from syspro_primex_opco_sub_ledger_type