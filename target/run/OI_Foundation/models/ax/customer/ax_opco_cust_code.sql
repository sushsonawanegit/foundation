begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_code ("OPCO_CUST_CODE_SK", "OPCO_CUST_CODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_CODE_CD", "SRC_CUST_CODE_DESC")
        (
            select "OPCO_CUST_CODE_SK", "OPCO_CUST_CODE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_CODE_CD", "SRC_CUST_CODE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_code__dbt_tmp
        );
    commit;