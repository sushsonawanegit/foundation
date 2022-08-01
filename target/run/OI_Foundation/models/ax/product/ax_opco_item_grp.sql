begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_grp ("OPCO_ITEM_GRP_SK", "OPCO_ITEM_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_GRP_CD", "OPCO_ID", "SRC_ITEM_GRP_DESC", "PRODN_UOM_SK", "SALES_UOM_SK", "OPCO_ITEM_FREIGHT_SK", "CAPEX_ONLY_IND", "CALC_CATEGORY_CD", "ITEM_GRP_TYPE_TXT", "EXPLICIT_FREIGHT_IND", "EXPLICIT_FREIGHT_PCT", "FREIGHT_PCT", "IC_MARKUP_PCT", "IMBEDDED_FREIGHT_IND", "IMBEDDED_FREIGHT_PCT", "MARKUP_PCT", "PROJECT_ONLY_IND", "PROJECT_CTGRY_CD")
        (
            select "OPCO_ITEM_GRP_SK", "OPCO_ITEM_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ITEM_GRP_CD", "OPCO_ID", "SRC_ITEM_GRP_DESC", "PRODN_UOM_SK", "SALES_UOM_SK", "OPCO_ITEM_FREIGHT_SK", "CAPEX_ONLY_IND", "CALC_CATEGORY_CD", "ITEM_GRP_TYPE_TXT", "EXPLICIT_FREIGHT_IND", "EXPLICIT_FREIGHT_PCT", "FREIGHT_PCT", "IC_MARKUP_PCT", "IMBEDDED_FREIGHT_IND", "IMBEDDED_FREIGHT_PCT", "MARKUP_PCT", "PROJECT_ONLY_IND", "PROJECT_CTGRY_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_item_grp__dbt_tmp
        );
    commit;