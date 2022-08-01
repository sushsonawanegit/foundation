begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand ("OPCO_BRAND_SK", "OPCO_BRAND_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_BRAND_CD", "SRC_BRAND_NM", "ACTV_IND", "BRAND_WO_SPCL_CHR_CD")
        (
            select "OPCO_BRAND_SK", "OPCO_BRAND_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_BRAND_CD", "SRC_BRAND_NM", "ACTV_IND", "BRAND_WO_SPCL_CHR_CD"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_brand__dbt_tmp
        );
    commit;