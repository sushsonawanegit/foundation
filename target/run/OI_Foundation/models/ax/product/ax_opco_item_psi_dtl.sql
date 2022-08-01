begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_psi_dtl ("OPCO_ITEM_PSI_DTL_SK", "OPCO_ITEM_PSI_DTL_CK", "OPCO_ITEM_PSI_DTL_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_CD", "OPCO_ID", "PSI_TYPE_TXT", "OPCO_ITEM_SK", "OPCO_UOM_SK", "PSI_PRICE_DT", "PSI_PRICE_AMT", "PSI_PRICE_UNIT_QTY", "PSI_DLVRY_QTY", "PSI_LINE_DSCNT_AMT")
        (
            select "OPCO_ITEM_PSI_DTL_SK", "OPCO_ITEM_PSI_DTL_CK", "OPCO_ITEM_PSI_DTL_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_CD", "OPCO_ID", "PSI_TYPE_TXT", "OPCO_ITEM_SK", "OPCO_UOM_SK", "PSI_PRICE_DT", "PSI_PRICE_AMT", "PSI_PRICE_UNIT_QTY", "PSI_DLVRY_QTY", "PSI_LINE_DSCNT_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_psi_dtl__dbt_tmp
        );
    commit;