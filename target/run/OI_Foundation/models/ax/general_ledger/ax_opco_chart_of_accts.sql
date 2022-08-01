begin;
    

        insert into OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts ("OPCO_CHART_OF_ACCTS_SK", "OPCO_CHART_OF_ACCTS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "GL_ACCT_NBR", "OPCO_ID", "GL_ACCT_NM", "CHART_OF_ACCTS_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_CHART_OF_ACCTS_TYPE_SK", "OPCO_UOM_SK", "GL_ACCT_ALIAS_NM", "MANUAL_ENTRY_ALLOWED_IND", "DPO_EXCLUSION_IND", "MOVEMENT_ALLOWED_IND", "COST_CENTER_REQD_TXT", "DEPT_REQD_TXT", "TYPE_REQD_TXT", "PRNT_GL_ACCT_NBR", "RELATED_GL_ACCT_NBR", "MONETARY_GL_ACCT_IND", "ACTV_IND", "BALANCE_SHEET_ACCT_IND", "SUMMARY_ACCT_IND", "OPCO_BRAND_SK", "ACCT_CLSSFCTN_CD", "SUB_ACCT_NBR")
        (
            select "OPCO_CHART_OF_ACCTS_SK", "OPCO_CHART_OF_ACCTS_CK", "CRT_DTM", "STG_LOAD_DTM", "DELETE_DTM", "SRC_SYS_NM", "GL_ACCT_NBR", "OPCO_ID", "GL_ACCT_NM", "CHART_OF_ACCTS_SK", "OPCO_COST_CENTER_SK", "OPCO_DEPT_SK", "OPCO_TYPE_SK", "OPCO_PURPOSE_SK", "OPCO_LOB_SK", "OPCO_CHART_OF_ACCTS_TYPE_SK", "OPCO_UOM_SK", "GL_ACCT_ALIAS_NM", "MANUAL_ENTRY_ALLOWED_IND", "DPO_EXCLUSION_IND", "MOVEMENT_ALLOWED_IND", "COST_CENTER_REQD_TXT", "DEPT_REQD_TXT", "TYPE_REQD_TXT", "PRNT_GL_ACCT_NBR", "RELATED_GL_ACCT_NBR", "MONETARY_GL_ACCT_IND", "ACTV_IND", "BALANCE_SHEET_ACCT_IND", "SUMMARY_ACCT_IND", "OPCO_BRAND_SK", "ACCT_CLSSFCTN_CD", "SUB_ACCT_NBR"
            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts__dbt_tmp
        );
    commit;