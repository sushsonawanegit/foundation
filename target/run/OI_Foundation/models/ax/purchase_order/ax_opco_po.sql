begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po ("OPCO_PO_SK", "OPCO_PO_CK", "OPCO_PO_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PO_ID", "PO_NM", "OPCO_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_VENDOR_SK", "INVOICE_VENDOR_SK", "OPCO_CASH_DSCNT_TERMS_SK", "TRANS_CURRENCY_SK", "OPCO_TRANS_STATUS_SK", "OPCO_PO_TYPE_SK", "OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_MODE_SK", "INTERCOMPANY_ORIG_SLS_ORDR_SK", "INTERCOMPANY_ORIG_CUST_SK", "OPCO_DLVRY_MODE_SK", "OPCO_DLVRY_TERMS_SK", "OPCO_PO_DOC_STATUS_SK", "WAREHOUSE_SK", "OPCO_PROJECT_SK", "OPCO_LOCN_SK", "DLVRY_DT", "PO_CRT_DTM", "PO_REQSTD_BY_NM", "VENDOR_GRP_CD", "DLVRY_VENDOR_NM", "VENDOR_REF_TXT", "ALIAS_NM", "ITEM_BUYER_GRP_ID", "PURCHASED_BY_TXT", "TAXABLE_IND", "DSCNT_PCT", "CASH_DSCNT_PCT")
        (
            select "OPCO_PO_SK", "OPCO_PO_CK", "OPCO_PO_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PO_ID", "PO_NM", "OPCO_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_VENDOR_SK", "INVOICE_VENDOR_SK", "OPCO_CASH_DSCNT_TERMS_SK", "TRANS_CURRENCY_SK", "OPCO_TRANS_STATUS_SK", "OPCO_PO_TYPE_SK", "OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_MODE_SK", "INTERCOMPANY_ORIG_SLS_ORDR_SK", "INTERCOMPANY_ORIG_CUST_SK", "OPCO_DLVRY_MODE_SK", "OPCO_DLVRY_TERMS_SK", "OPCO_PO_DOC_STATUS_SK", "WAREHOUSE_SK", "OPCO_PROJECT_SK", "OPCO_LOCN_SK", "DLVRY_DT", "PO_CRT_DTM", "PO_REQSTD_BY_NM", "VENDOR_GRP_CD", "DLVRY_VENDOR_NM", "VENDOR_REF_TXT", "ALIAS_NM", "ITEM_BUYER_GRP_ID", "PURCHASED_BY_TXT", "TAXABLE_IND", "DSCNT_PCT", "CASH_DSCNT_PCT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po__dbt_tmp
        );
    commit;