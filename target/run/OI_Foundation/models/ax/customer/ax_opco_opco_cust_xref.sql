begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_cust_xref ("OPCO_OPCO_CUST_XREF_CK", "OPCO_SK", "OPCO_CUST_SK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "ACTV_IND", "OPCO_CURRENCY_SK", "OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_MODE_SK", "OPCO_CUST_CODE_SK", "OPCO_CUST_TYPE_SK", "OPCO_COMMSN_GRP_SK", "OPCO_CUST_GRP_SK", "OPCO_CASH_DSCNT_TERMS_SK", "LINE_DISC_TXT", "MAX_CR_AMT", "MANDATORY_CR_LMT_IND", "BLOCK_LVL_CD", "CUST_CLSSFCTN_CD", "PYMNT_SCHD_DESC", "SLS_GRP_NM", "RGNL_SLS_MGR_NM", "INVOICE_ACCT_NBR", "SEGMENT_CD", "SEGMENTATION_TXT")
        (
            select "OPCO_OPCO_CUST_XREF_CK", "OPCO_SK", "OPCO_CUST_SK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "ACTV_IND", "OPCO_CURRENCY_SK", "OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_MODE_SK", "OPCO_CUST_CODE_SK", "OPCO_CUST_TYPE_SK", "OPCO_COMMSN_GRP_SK", "OPCO_CUST_GRP_SK", "OPCO_CASH_DSCNT_TERMS_SK", "LINE_DISC_TXT", "MAX_CR_AMT", "MANDATORY_CR_LMT_IND", "BLOCK_LVL_CD", "CUST_CLSSFCTN_CD", "PYMNT_SCHD_DESC", "SLS_GRP_NM", "RGNL_SLS_MGR_NM", "INVOICE_ACCT_NBR", "SEGMENT_CD", "SEGMENTATION_TXT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_opco_cust_xref__dbt_tmp
        );
    commit;