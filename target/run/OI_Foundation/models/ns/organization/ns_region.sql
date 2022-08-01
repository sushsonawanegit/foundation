begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_region ("REGION_SK", "REGION_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "REGION_ID", "REGION_NM", "ACTV_IND")
        (
            select "REGION_SK", "REGION_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "REGION_ID", "REGION_NM", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_region__dbt_tmp
        );
    commit;