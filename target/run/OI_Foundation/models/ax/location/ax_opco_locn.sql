begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn ("OPCO_LOCN_SK", "OPCO_LOCN_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "STD_LOCN_SK", "SRC_SYS_NM", "ADDR_LN_1_TXT", "ADDR_LN_2_TXT", "ADDR_LN_3_TXT", "CITY_NM", "STATE_NM", "COUNTRY_NM", "ZIP_CD", "COUNTY_NM")
        (
            select "OPCO_LOCN_SK", "OPCO_LOCN_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "STD_LOCN_SK", "SRC_SYS_NM", "ADDR_LN_1_TXT", "ADDR_LN_2_TXT", "ADDR_LN_3_TXT", "CITY_NM", "STATE_NM", "COUNTRY_NM", "ZIP_CD", "COUNTY_NM"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_locn__dbt_tmp
        );
    commit;