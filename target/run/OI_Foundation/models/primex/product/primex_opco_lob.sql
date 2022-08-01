begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob ("OPCO_LOB_SK", "OPCO_LOB_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_LOB_CD", "SRC_LOB_NM", "ACTV_IND", "LOB_WO_SPCL_CHR_CD")
        (
            select "OPCO_LOB_SK", "OPCO_LOB_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_LOB_CD", "SRC_LOB_NM", "ACTV_IND", "LOB_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_lob__dbt_tmp
        );
    commit;