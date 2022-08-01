begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_type ("OPCO_CUST_TYPE_SK", "OPCO_CUST_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_TYPE_CD", "SRC_CUST_TYPE_DESC")
        (
            select "OPCO_CUST_TYPE_SK", "OPCO_CUST_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_TYPE_CD", "SRC_CUST_TYPE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_type__dbt_tmp
        );
    commit;