begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip ("OPCO_VENDOR_PACKING_SLIP_SK", "OPCO_VENDOR_PACKING_SLIP_CK", "OPCO_VENDOR_PACKING_SLIP_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PACKING_SLIP_ID", "INTERNAL_PACKING_SLIP_ID", "OPCO_SK", "OPCO_VENDOR_SK", "OPCO_INVOICE_VENDOR_SK", "OPCO_PO_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_DLVRY_TERMS_SK", "OPCO_DLVRY_MODE_SK", "DLVRY_LOCN_SK", "OPCO_PROJECT_SK", "OPCO_PO_TYPE_SK", "DLVRY_DT", "DLVRY_VENDOR_NM", "GL_VOUCHER_NBR", "ITEM_BUYER_GRP_ID", "PURCHASER_NM", "RQSTD_BY_NM", "REQUSITIONER_NM", "ATTN_INFO_TXT", "DLVRY_QTY", "DLVRY_VOLUME_AMT", "DLVRY_WEIGHT_AMT")
        (
            select "OPCO_VENDOR_PACKING_SLIP_SK", "OPCO_VENDOR_PACKING_SLIP_CK", "OPCO_VENDOR_PACKING_SLIP_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PACKING_SLIP_ID", "INTERNAL_PACKING_SLIP_ID", "OPCO_SK", "OPCO_VENDOR_SK", "OPCO_INVOICE_VENDOR_SK", "OPCO_PO_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_DLVRY_TERMS_SK", "OPCO_DLVRY_MODE_SK", "DLVRY_LOCN_SK", "OPCO_PROJECT_SK", "OPCO_PO_TYPE_SK", "DLVRY_DT", "DLVRY_VENDOR_NM", "GL_VOUCHER_NBR", "ITEM_BUYER_GRP_ID", "PURCHASER_NM", "RQSTD_BY_NM", "REQUSITIONER_NM", "ATTN_INFO_TXT", "DLVRY_QTY", "DLVRY_VOLUME_AMT", "DLVRY_WEIGHT_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip__dbt_tmp
        );
    commit;