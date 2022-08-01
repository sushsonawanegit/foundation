begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_mode ("OPCO_PYMNT_MODE_SK", "OPCO_PYMNT_MODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PYMNT_MODE_CD", "SRC_PYMNT_MODE_DESC")
        (
            select "OPCO_PYMNT_MODE_SK", "OPCO_PYMNT_MODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PYMNT_MODE_CD", "SRC_PYMNT_MODE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_mode__dbt_tmp
        );
    commit;