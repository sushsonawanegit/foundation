with v_opco_sub_ledger_type as(
    {{ union_relations(
        relations=[ref('jde_hnck_opco_sub_ledger_type'), ref('syspro_primex_opco_sub_ledger_type')])}}
)

select * from v_opco_sub_ledger_type