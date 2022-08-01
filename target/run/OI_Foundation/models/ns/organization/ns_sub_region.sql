begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_sub_region ("SUB_REGION_SK", "SUB_REGION_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SUB_REGION_ID", "SUB_REGION_NM", "ACTV_IND")
        (
            select "SUB_REGION_SK", "SUB_REGION_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SUB_REGION_ID", "SUB_REGION_NM", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_sub_region__dbt_tmp
        );
    commit;