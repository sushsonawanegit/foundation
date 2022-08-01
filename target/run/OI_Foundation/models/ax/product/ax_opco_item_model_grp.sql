begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_model_grp ("OPCO_ITEM_MODEL_GRP_SK", "OPCO_ITEM_MODEL_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_MODEL_GRP_CD", "SRC_ITEM_MODEL_GRP_NM")
        (
            select "OPCO_ITEM_MODEL_GRP_SK", "OPCO_ITEM_MODEL_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_MODEL_GRP_CD", "SRC_ITEM_MODEL_GRP_NM"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_model_grp__dbt_tmp
        );
    commit;