begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_item_trans ("OPCO_PROJECT_ITEM_TRANS_SK", "OPCO_PROJECT_ITEM_TRANS_CK", "OPCO_PROJECT_ITEM_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PROJECT_ITEM_TRANS_ID", "INVTRY_TRANS_ID", "OPCO_PROJECT_SK", "OPCO_SK", "OPCO_ITEM_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_PROJECT_CATGRY_SK", "TRANS_UOM_SK", "OPCO_ASSCTN_SK", "OPCO_PROJECT_TRANS_STATUS_SK", "OPCO_PROJECT_TRANS_ORIGIN_SK", "TRANS_DT", "TRANS_DESC", "LINE_PROPERTY_ID", "TAX_GRP_ID", "TAX_ITEM_GRP_ID", "PACKING_SLIP_VOUCHER_NBR", "CUST_INVOICE_NBR", "PROJECT_ADJSMT_REF_ID", "REF_PROJECT_ITEM_TRANS_ID", "CALC_IND", "PROJECT_COST_TRANS_CONVERSION_IND", "TRANS_QTY", "PER_UNIT_SLS_PRICE_AMT", "ITEM_PRICE_AMT", "TOTAL_REVENUE_AMT", "TOTAL_COST_AMT", "TOTAL_STD_COST_AMT", "MARGIN_CONTRIBUTION_PCT")
        (
            select "OPCO_PROJECT_ITEM_TRANS_SK", "OPCO_PROJECT_ITEM_TRANS_CK", "OPCO_PROJECT_ITEM_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PROJECT_ITEM_TRANS_ID", "INVTRY_TRANS_ID", "OPCO_PROJECT_SK", "OPCO_SK", "OPCO_ITEM_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_PROJECT_CATGRY_SK", "TRANS_UOM_SK", "OPCO_ASSCTN_SK", "OPCO_PROJECT_TRANS_STATUS_SK", "OPCO_PROJECT_TRANS_ORIGIN_SK", "TRANS_DT", "TRANS_DESC", "LINE_PROPERTY_ID", "TAX_GRP_ID", "TAX_ITEM_GRP_ID", "PACKING_SLIP_VOUCHER_NBR", "CUST_INVOICE_NBR", "PROJECT_ADJSMT_REF_ID", "REF_PROJECT_ITEM_TRANS_ID", "CALC_IND", "PROJECT_COST_TRANS_CONVERSION_IND", "TRANS_QTY", "PER_UNIT_SLS_PRICE_AMT", "ITEM_PRICE_AMT", "TOTAL_REVENUE_AMT", "TOTAL_COST_AMT", "TOTAL_STD_COST_AMT", "MARGIN_CONTRIBUTION_PCT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_item_trans__dbt_tmp
        );
    commit;