begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_type ("OPCO_TYPE_SK", "OPCO_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_TYPE_CD", "SRC_TYPE_DESC", "ACTV_IND", "TYPE_WO_SPCL_CHR_CD")
        (
            select "OPCO_TYPE_SK", "OPCO_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_TYPE_CD", "SRC_TYPE_DESC", "ACTV_IND", "TYPE_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_type__dbt_tmp
        );
    commit;