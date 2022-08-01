begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type ("OPCO_SUB_LEDGER_TYPE_SK", "OPCO_SUB_LEDGER_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_SUB_LEDGER_TYPE_CD", "SRC_SUB_LEDGER_TYPE_DESC", "ACTV_IND")
        (
            select "OPCO_SUB_LEDGER_TYPE_SK", "OPCO_SUB_LEDGER_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_SUB_LEDGER_TYPE_CD", "SRC_SUB_LEDGER_TYPE_DESC", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_sub_ledger_type__dbt_tmp
        );
    commit;