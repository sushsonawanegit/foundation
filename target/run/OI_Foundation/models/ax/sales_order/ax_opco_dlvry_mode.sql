begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode ("OPCO_DLVRY_MODE_SK", "OPCO_DLVRY_MODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DLVRY_MODE_CD", "SRC_DLVRY_MODE_DESC")
        (
            select "OPCO_DLVRY_MODE_SK", "OPCO_DLVRY_MODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DLVRY_MODE_CD", "SRC_DLVRY_MODE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_mode__dbt_tmp
        );
    commit;