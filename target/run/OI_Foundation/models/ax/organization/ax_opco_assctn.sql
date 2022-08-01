begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn ("OPCO_ASSCTN_SK", "OPCO_ASSCTN_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ASSCTN_CD", "OPCO_ID", "OPCO_SK", "OPCO_SITE_SK", "COST_CENTER_SK", "WAREHOUSE_SK", "ACTV_IND")
        (
            select "OPCO_ASSCTN_SK", "OPCO_ASSCTN_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ASSCTN_CD", "OPCO_ID", "OPCO_SK", "OPCO_SITE_SK", "COST_CENTER_SK", "WAREHOUSE_SK", "ACTV_IND"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_assctn__dbt_tmp
        );
    commit;