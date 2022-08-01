begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type ("OPCO_TYPE_SK", "OPCO_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_TYPE_CD", "SRC_TYPE_DESC", "TYPE_WO_SPCL_CHR_CD", "ACTV_IND")
        (
            select "OPCO_TYPE_SK", "OPCO_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_TYPE_CD", "SRC_TYPE_DESC", "TYPE_WO_SPCL_CHR_CD", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_type__dbt_tmp
        );
    commit;