begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_revenue_trans ("OPCO_PROJECT_REVENUE_TRANS_SK", "OPCO_PROJECT_REVENUE_TRANS_CK", "OPCO_PROJECT_REVENUE_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_KEY_TXT", "OPCO_PROJECT_SK", "OPCO_SK", "CHG_NBR", "PROJECT_REVENUE_TRANS_TYPE_CD", "PROJECT_REVENUE_TRANS_DESC", "TRANS_DT", "APPRVD_IND", "PROJECT_REVENUE_TRANS_AMT")
        (
            select "OPCO_PROJECT_REVENUE_TRANS_SK", "OPCO_PROJECT_REVENUE_TRANS_CK", "OPCO_PROJECT_REVENUE_TRANS_AK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "SRC_KEY_TXT", "OPCO_PROJECT_SK", "OPCO_SK", "CHG_NBR", "PROJECT_REVENUE_TRANS_TYPE_CD", "PROJECT_REVENUE_TRANS_DESC", "TRANS_DT", "APPRVD_IND", "PROJECT_REVENUE_TRANS_AMT"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_revenue_trans__dbt_tmp
        );
    commit;