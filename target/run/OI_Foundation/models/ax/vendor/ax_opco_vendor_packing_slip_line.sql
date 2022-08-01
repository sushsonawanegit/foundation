begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip_line ("OPCO_VENDOR_PACKING_SLIP_LINE_SK", "OPCO_VENDOR_PACKING_SLIP_LINE_CK", "OPCO_VENDOR_PACKING_SLIP_LINE_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PACKING_SLIP_ID", "INTERNAL_PACKING_SLIP_ID", "INVTRY_TRANS_ID", "OPCO_VENDOR_PACKING_SLIP_SK", "OPCO_SK", "OPCO_ITEM_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_PO_SK", "ORIG_PO_SK", "OPCO_ASSCTN_SK", "OPCO_UOM_SK", "OPCO_INVTRY_REF_TYPE_SK", "LINE_NBR", "DLVRY_DT", "INVTRY_TRANS_DT", "INVTRY_REF_ID", "VENDOR_ITEM_ID", "ITEM_DESC", "PARTIAL_DLVRY_IND", "DEST_COUNTRY_NM", "DEST_STATE_NM", "DEST_COUNTY_NM", "PER_UNIT_QTY", "ORDR_QTY", "RECEIVED_QTY", "REMAINING_QTY", "INVTRY_QTY", "OPCO_CURRENCY_TRANS_AMT")
        (
            select "OPCO_VENDOR_PACKING_SLIP_LINE_SK", "OPCO_VENDOR_PACKING_SLIP_LINE_CK", "OPCO_VENDOR_PACKING_SLIP_LINE_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "PACKING_SLIP_ID", "INTERNAL_PACKING_SLIP_ID", "INVTRY_TRANS_ID", "OPCO_VENDOR_PACKING_SLIP_SK", "OPCO_SK", "OPCO_ITEM_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_PO_SK", "ORIG_PO_SK", "OPCO_ASSCTN_SK", "OPCO_UOM_SK", "OPCO_INVTRY_REF_TYPE_SK", "LINE_NBR", "DLVRY_DT", "INVTRY_TRANS_DT", "INVTRY_REF_ID", "VENDOR_ITEM_ID", "ITEM_DESC", "PARTIAL_DLVRY_IND", "DEST_COUNTRY_NM", "DEST_STATE_NM", "DEST_COUNTY_NM", "PER_UNIT_QTY", "ORDR_QTY", "RECEIVED_QTY", "REMAINING_QTY", "INVTRY_QTY", "OPCO_CURRENCY_TRANS_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_packing_slip_line__dbt_tmp
        );
    commit;