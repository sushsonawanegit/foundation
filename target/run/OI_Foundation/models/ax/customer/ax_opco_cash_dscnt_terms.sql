begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms ("OPCO_CASH_DSCNT_TERMS_SK", "OPCO_CASH_DSCNT_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CASH_DSCNT_TERMS_CD", "SRC_CASH_DSCNT_TERMS_DESC", "PYMNT_PERIOD_IN_DAYS_NBR", "DSCNT_PCT")
        (
            select "OPCO_CASH_DSCNT_TERMS_SK", "OPCO_CASH_DSCNT_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CASH_DSCNT_TERMS_CD", "SRC_CASH_DSCNT_TERMS_DESC", "PYMNT_PERIOD_IN_DAYS_NBR", "DSCNT_PCT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cash_dscnt_terms__dbt_tmp
        );
    commit;