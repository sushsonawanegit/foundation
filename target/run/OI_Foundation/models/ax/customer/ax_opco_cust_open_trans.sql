begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_open_trans ("OPCO_CUST_OPEN_TRANS_SK", "OPCO_CUST_OPEN_TRANS_CK", "OPCO_CUST_OPEN_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "SRC_KEY_TXT", "OPCO_CUST_TRANS_SK", "OPCO_SK", "OPCO_CUST_SK", "CASH_DSCNT_GL_ACCT_SK", "TRANS_DT", "DUE_DT", "LTST_INTEREST_CALC_DT", "CASH_DSCNT_DT", "CASH_DSCNT_USAGE_TXT", "TRANS_CURRENCY_TRANS_AMT", "OPCO_CURRENCY_TRANS_AMT", "INCLUDED_CASH_DSCNT_AMT")
        (
            select "OPCO_CUST_OPEN_TRANS_SK", "OPCO_CUST_OPEN_TRANS_CK", "OPCO_CUST_OPEN_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "SRC_KEY_TXT", "OPCO_CUST_TRANS_SK", "OPCO_SK", "OPCO_CUST_SK", "CASH_DSCNT_GL_ACCT_SK", "TRANS_DT", "DUE_DT", "LTST_INTEREST_CALC_DT", "CASH_DSCNT_DT", "CASH_DSCNT_USAGE_TXT", "TRANS_CURRENCY_TRANS_AMT", "OPCO_CURRENCY_TRANS_AMT", "INCLUDED_CASH_DSCNT_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_open_trans__dbt_tmp
        );
    commit;