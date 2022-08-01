begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans ("OPCO_GL_TRANS_SK", "OPCO_GL_TRANS_CK", "OPCO_GL_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_KEY_TXT", "TRANS_DT", "OPCO_SK", "OPCO_CHART_OF_ACCTS_SK", "OPCO_GL_TRANS_TYPE_SK", "OPCO_GL_TRANS_POSTING_TYPE_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "TRANS_CURRENCY_SK", "OPCO_PROJ_SK", "ORIGINAL_GL_OPCO_SK", "ORIGINAL_GL_VOUCHER_NBR", "VOUCHER_NBR", "JOURNAL_BATCH_NBR", "GL_DESC", "DOCUMENT_NBR", "DOCUMENT_DT", "CRRCTN_TRANS_IND", "CREDIT_IND", "GL_QTY", "TRANS_CURRENCY_GL_AMT", "OPCO_CURRENCY_GL_AMT", "FSCL_YR_NBR", "FSCL_MNTH_NBR", "POSTED_IND", "OPCO_UOM_SK", "OPCO_BRAND_SK", "OPCO_SUB_LEDGER_TYPE_SK", "SUB_LEDGER_CD")
        (
            select "OPCO_GL_TRANS_SK", "OPCO_GL_TRANS_CK", "OPCO_GL_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_KEY_TXT", "TRANS_DT", "OPCO_SK", "OPCO_CHART_OF_ACCTS_SK", "OPCO_GL_TRANS_TYPE_SK", "OPCO_GL_TRANS_POSTING_TYPE_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "TRANS_CURRENCY_SK", "OPCO_PROJ_SK", "ORIGINAL_GL_OPCO_SK", "ORIGINAL_GL_VOUCHER_NBR", "VOUCHER_NBR", "JOURNAL_BATCH_NBR", "GL_DESC", "DOCUMENT_NBR", "DOCUMENT_DT", "CRRCTN_TRANS_IND", "CREDIT_IND", "GL_QTY", "TRANS_CURRENCY_GL_AMT", "OPCO_CURRENCY_GL_AMT", "FSCL_YR_NBR", "FSCL_MNTH_NBR", "POSTED_IND", "OPCO_UOM_SK", "OPCO_BRAND_SK", "OPCO_SUB_LEDGER_TYPE_SK", "SUB_LEDGER_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans__dbt_tmp
        );
    commit;