with v_opco_sub_ledger_type as(
    {{ union_relations(
        relations=[ref('jde_opco_sub_ledger_type'), ref('primex_opco_sub_ledger_type')])}}
)

select * from v_opco_sub_ledger_type