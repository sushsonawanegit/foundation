begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_size ("OPCO_ITEM_SIZE_SK", "OPCO_ITEM_SIZE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SIZE_CD", "OPCO_ID", "SRC_ITEM_CD", "SRC_ITEM_SK", "SRC_ITEM_SIZE_NM", "SRC_ITEM_SIZE_DESC")
        (
            select "OPCO_ITEM_SIZE_SK", "OPCO_ITEM_SIZE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SIZE_CD", "OPCO_ID", "SRC_ITEM_CD", "SRC_ITEM_SK", "SRC_ITEM_SIZE_NM", "SRC_ITEM_SIZE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_size__dbt_tmp
        );
    commit;