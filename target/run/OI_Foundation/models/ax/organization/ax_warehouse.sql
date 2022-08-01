begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse ("WAREHOUSE_SK", "WAREHOUSE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "WAREHOUSE_ID", "WAREHOUSE_NM", "ACTV_IND")
        (
            select "WAREHOUSE_SK", "WAREHOUSE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "OPCO_ID", "WAREHOUSE_ID", "WAREHOUSE_NM", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_warehouse__dbt_tmp
        );
    commit;