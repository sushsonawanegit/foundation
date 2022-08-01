begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site ("OPCO_SITE_SK", "OPCO_SITE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_SITE_ID", "SRC_SITE_NM", "ACTV_IND")
        (
            select "OPCO_SITE_SK", "OPCO_SITE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_SITE_ID", "SRC_SITE_NM", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_site__dbt_tmp
        );
    commit;