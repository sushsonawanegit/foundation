begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts_type ("OPCO_CHART_OF_ACCTS_TYPE_SK", "OPCO_CHART_OF_ACCTS_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ACCT_TYPE_CD", "SRC_ACCT_TYPE_DESC")
        (
            select "OPCO_CHART_OF_ACCTS_TYPE_SK", "OPCO_CHART_OF_ACCTS_TYPE_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_ACCT_TYPE_CD", "SRC_ACCT_TYPE_DESC"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts_type__dbt_tmp
        );
    commit;