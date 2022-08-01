begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_settled_trans ("OPCO_CUST_SETTLED_TRANS_SK", "OPCO_CUST_SETTLED_TRANS_CK", "OPCO_CUST_SETTLED_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "SRC_KEY_TXT", "OPCO_SK", "OPCO_CUST_TRANS_SK", "OPCO_CUST_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "CASH_DSCNT_GL_ACCT_SK", "TRANS_DT", "DUE_DT", "SRC_CRT_DTM", "OFFSET_CUST_SK", "OFFSET_CUST_TRANS_SK", "OFFSET_VOUCHER_NBR", "CUST_CASH_DSCNT_DT", "LATEST_INTEREST_CALC_DT", "REVERSIBLE_IND", "SETTLEMENT_GRP_TXT", "SETTLED_VOUCHER_NBR", "TRANS_CURRENCY_SETTLED_AMT", "OPCO_CURRENCY_SETTLED_AMT", "EXCH_ADJSTMNT_AMT", "UTILIZED_CASH_DSCNT_AMT")
        (
            select "OPCO_CUST_SETTLED_TRANS_SK", "OPCO_CUST_SETTLED_TRANS_CK", "OPCO_CUST_SETTLED_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "SRC_KEY_TXT", "OPCO_SK", "OPCO_CUST_TRANS_SK", "OPCO_CUST_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "CASH_DSCNT_GL_ACCT_SK", "TRANS_DT", "DUE_DT", "SRC_CRT_DTM", "OFFSET_CUST_SK", "OFFSET_CUST_TRANS_SK", "OFFSET_VOUCHER_NBR", "CUST_CASH_DSCNT_DT", "LATEST_INTEREST_CALC_DT", "REVERSIBLE_IND", "SETTLEMENT_GRP_TXT", "SETTLED_VOUCHER_NBR", "TRANS_CURRENCY_SETTLED_AMT", "OPCO_CURRENCY_SETTLED_AMT", "EXCH_ADJSTMNT_AMT", "UTILIZED_CASH_DSCNT_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_settled_trans__dbt_tmp
        );
    commit;