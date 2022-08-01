begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_purpose ("OPCO_PURPOSE_SK", "OPCO_PURPOSE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PURPOSE_CD", "SRC_PURPOSE_DESC", "ACTV_IND", "PURPOSE_WO_SPCL_CHR_CD")
        (
            select "OPCO_PURPOSE_SK", "OPCO_PURPOSE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PURPOSE_CD", "SRC_PURPOSE_DESC", "ACTV_IND", "PURPOSE_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_purpose__dbt_tmp
        );
    commit;