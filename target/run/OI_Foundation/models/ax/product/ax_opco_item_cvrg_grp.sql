begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_cvrg_grp ("OPCO_ITEM_CVRG_GRP_SK", "OPCO_ITEM_CVRG_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_CVRG_GRP_CD", "OPCO_ID", "SRC_ITEM_CVRG_GRP_DESC")
        (
            select "OPCO_ITEM_CVRG_GRP_SK", "OPCO_ITEM_CVRG_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_CVRG_GRP_CD", "OPCO_ID", "SRC_ITEM_CVRG_GRP_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_cvrg_grp__dbt_tmp
        );
    commit;