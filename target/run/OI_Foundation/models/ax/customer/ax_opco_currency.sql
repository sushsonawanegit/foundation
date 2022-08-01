begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency ("OPCO_CURRENCY_SK", "OPCO_CURRENCY_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CURRENCY_CD", "SRC_CURRENCY_NM")
        (
            select "OPCO_CURRENCY_SK", "OPCO_CURRENCY_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CURRENCY_CD", "SRC_CURRENCY_NM"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_currency__dbt_tmp
        );
    commit;