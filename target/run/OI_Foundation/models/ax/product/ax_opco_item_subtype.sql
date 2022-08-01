begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_subtype ("OPCO_ITEM_SUBTYPE_SK", "OPCO_ITEM_SUBTYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SUBTYPE_CD", "SRC_ITEM_TYPE_CD", "SRC_ITEM_SUBTYPE_DESC")
        (
            select "OPCO_ITEM_SUBTYPE_SK", "OPCO_ITEM_SUBTYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SUBTYPE_CD", "SRC_ITEM_TYPE_CD", "SRC_ITEM_SUBTYPE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_subtype__dbt_tmp
        );
    commit;