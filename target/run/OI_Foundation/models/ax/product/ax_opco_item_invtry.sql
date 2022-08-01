begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_invtry ("OPCO_ITEM_SK", "OPCO_ASSCTN_SK", "OPCO_ITEM_INVTRY_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "ITEM_AVBL_PHYSICAL_QTY", "ITEM_POSTED_QTY", "ITEM_POSTED_VALUE_AMT")
        (
            select "OPCO_ITEM_SK", "OPCO_ASSCTN_SK", "OPCO_ITEM_INVTRY_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "ITEM_AVBL_PHYSICAL_QTY", "ITEM_POSTED_QTY", "ITEM_POSTED_VALUE_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_invtry__dbt_tmp
        );
    commit;