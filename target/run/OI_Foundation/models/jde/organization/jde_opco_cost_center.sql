begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center ("OPCO_COST_CENTER_SK", "OPCO_COST_CENTER_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_COST_CENTER_CD", "SRC_COST_CENTER_DESC", "ACTV_IND", "SRC_COST_CENTER_TYPE_TXT", "COST_CENTER_WO_SPCL_CHR_CD")
        (
            select "OPCO_COST_CENTER_SK", "OPCO_COST_CENTER_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_COST_CENTER_CD", "SRC_COST_CENTER_DESC", "ACTV_IND", "SRC_COST_CENTER_TYPE_TXT", "COST_CENTER_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_cost_center__dbt_tmp
        );
    commit;