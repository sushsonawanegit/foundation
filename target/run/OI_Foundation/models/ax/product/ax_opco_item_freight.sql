begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_freight ("OPCO_ITEM_FREIGHT_SK", "OPCO_ITEM_FREIGHT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_FREIGHT_CD", "OPCO_ID", "SRC_ITEM_FREIGHT_NM")
        (
            select "OPCO_ITEM_FREIGHT_SK", "OPCO_ITEM_FREIGHT_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_FREIGHT_CD", "OPCO_ID", "SRC_ITEM_FREIGHT_NM"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_freight__dbt_tmp
        );
    commit;