begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob ("OPCO_LOB_SK", "OPCO_LOB_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_LOB_CD", "SRC_LOB_NM", "LOB_WO_SPCL_CHR_CD", "ACTV_IND")
        (
            select "OPCO_LOB_SK", "OPCO_LOB_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_LOB_CD", "SRC_LOB_NM", "LOB_WO_SPCL_CHR_CD", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_lob__dbt_tmp
        );
    commit;