with opco_sub_ledger_type as(
    select *
    from {{ ref('v_opco_sub_ledger_type_lineage') }}
)

select * from opco_sub_ledger_type