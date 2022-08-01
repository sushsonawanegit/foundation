begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms ("OPCO_DLVRY_TERMS_SK", "OPCO_DLVRY_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DLVRY_TERMS_CD", "SRC_DLVRY_TERMS_DESC")
        (
            select "OPCO_DLVRY_TERMS_SK", "OPCO_DLVRY_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_DLVRY_TERMS_CD", "SRC_DLVRY_TERMS_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_dlvry_terms__dbt_tmp
        );
    commit;