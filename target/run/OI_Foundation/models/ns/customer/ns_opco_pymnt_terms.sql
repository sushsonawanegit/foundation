begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_terms ("OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PYMNT_TERMS_CD", "SRC_PYMNT_TERMS_DESC")
        (
            select "OPCO_PYMNT_TERMS_SK", "OPCO_PYMNT_TERMS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_PYMNT_TERMS_CD", "SRC_PYMNT_TERMS_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_pymnt_terms__dbt_tmp
        );
    commit;