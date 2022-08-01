begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_grp ("OPCO_CUST_GRP_SK", "OPCO_CUST_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_GRP_CD", "SRC_CUST_GRP_DESC")
        (
            select "OPCO_CUST_GRP_SK", "OPCO_CUST_GRP_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_CUST_GRP_CD", "SRC_CUST_GRP_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_grp__dbt_tmp
        );
    commit;