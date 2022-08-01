with opco_sub_ledger_type as(
    select *
    from OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sub_ledger_type_lineage
)

select * from opco_sub_ledger_type