begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_sls_class ("OPCO_ITEM_SLS_CLASS_SK", "OPCO_ITEM_SLS_CLASS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SLS_CLASS_CD", "SRC_ITEM_SLS_CLASS_DESC")
        (
            select "OPCO_ITEM_SLS_CLASS_SK", "OPCO_ITEM_SLS_CLASS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_SLS_CLASS_CD", "SRC_ITEM_SLS_CLASS_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_sls_class__dbt_tmp
        );
    commit;