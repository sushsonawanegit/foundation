begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_line ("OPCO_PO_LINE_SK", "OPCO_PO_LINE_CK", "OPCO_PO_LINE_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PO_ID", "PO_LINE_NBR", "OPCO_PO_SK", "OPCO_VENDOR_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_ASSCTN_SK", "OPCO_ITEM_SK", "TRANS_CURRENCY_SK", "OPCO_TRANS_STATUS_SK", "OPCO_PO_TYPE_SK", "PURCHASE_UOM_SK", "OPCO_GL_ACCT_SK", "OPCO_INVTRY_REF_TYPE_SK", "OPCO_PROJECT_CATGRY_SK", "OPCO_PROJECT_SK", "INVTRY_TRANS_ID", "INVTRY_REF_ID", "INVTRY_REF_TRANS_ID", "PROJECT_ITEM_TRANS_ID", "REQ_PLAN_SCHEDULE_ID", "REQ_PLAN_ORDR_ID", "ASSET_ID", "PROJECT_TAX_GRP_ID", "PO_LINE_NM", "PO_LINE_LOCKED_IND", "PO_LINE_DLVRY_COMPLETE_IND", "SCRAP_IND", "TAXABLE_IND", "TAX_ITEM_GRP_CD", "RQST_DLVRY_DT", "ACTL_DLVRY_DT", "DLVRY_TYPE_TXT", "PO_LINE_CRT_DTM", "PO_LINE_CRT_BY", "VENDOR_GRP_CD", "VENDOR_ITEM_ID", "INVTRY_UNIT_ORDR_QTY", "PURCHASE_UNIT_ORDR_QTY", "PURCHASE_UNIT_FINANCIAL_REMAINING_QTY", "PURCHASE_UNIT_PHYSICAL_REMAINING_QTY", "INVTRY_UNIT_PHYSICAL_REMAINING_QTY", "INVTRY_UNIT_FINANCIAL_REMAINING_QTY", "PURCHASE_UNIT_RECEIVED_QTY", "INVTRY_UNIT_RECEIVED_QTY", "PER_UNIT_QTY", "PO_LINE_DSCNT_PCT", "PER_UNIT_LINE_DSCNT_AMT", "PER_UNIT_MULTI_LINE_DSCNT_AMT", "UNIT_PURCHASE_PRICE_AMT", "PO_LINE_AMT", "PURCHASE_MARKUP_AMT", "TAX_1099_AMT")
        (
            select "OPCO_PO_LINE_SK", "OPCO_PO_LINE_CK", "OPCO_PO_LINE_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PO_ID", "PO_LINE_NBR", "OPCO_PO_SK", "OPCO_VENDOR_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_ASSCTN_SK", "OPCO_ITEM_SK", "TRANS_CURRENCY_SK", "OPCO_TRANS_STATUS_SK", "OPCO_PO_TYPE_SK", "PURCHASE_UOM_SK", "OPCO_GL_ACCT_SK", "OPCO_INVTRY_REF_TYPE_SK", "OPCO_PROJECT_CATGRY_SK", "OPCO_PROJECT_SK", "INVTRY_TRANS_ID", "INVTRY_REF_ID", "INVTRY_REF_TRANS_ID", "PROJECT_ITEM_TRANS_ID", "REQ_PLAN_SCHEDULE_ID", "REQ_PLAN_ORDR_ID", "ASSET_ID", "PROJECT_TAX_GRP_ID", "PO_LINE_NM", "PO_LINE_LOCKED_IND", "PO_LINE_DLVRY_COMPLETE_IND", "SCRAP_IND", "TAXABLE_IND", "TAX_ITEM_GRP_CD", "RQST_DLVRY_DT", "ACTL_DLVRY_DT", "DLVRY_TYPE_TXT", "PO_LINE_CRT_DTM", "PO_LINE_CRT_BY", "VENDOR_GRP_CD", "VENDOR_ITEM_ID", "INVTRY_UNIT_ORDR_QTY", "PURCHASE_UNIT_ORDR_QTY", "PURCHASE_UNIT_FINANCIAL_REMAINING_QTY", "PURCHASE_UNIT_PHYSICAL_REMAINING_QTY", "INVTRY_UNIT_PHYSICAL_REMAINING_QTY", "INVTRY_UNIT_FINANCIAL_REMAINING_QTY", "PURCHASE_UNIT_RECEIVED_QTY", "INVTRY_UNIT_RECEIVED_QTY", "PER_UNIT_QTY", "PO_LINE_DSCNT_PCT", "PER_UNIT_LINE_DSCNT_AMT", "PER_UNIT_MULTI_LINE_DSCNT_AMT", "UNIT_PURCHASE_PRICE_AMT", "PO_LINE_AMT", "PURCHASE_MARKUP_AMT", "TAX_1099_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_po_line__dbt_tmp
        );
    commit;