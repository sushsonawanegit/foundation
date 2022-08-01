with v_chart_of_accts as(
    
    

        (
            select

                
                    cast("CHART_OF_ACCTS_SK" as character varying(32)) as "CHART_OF_ACCTS_SK" ,
                    cast("CHART_OF_ACCTS_CK" as character varying(32)) as "CHART_OF_ACCTS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("GL_ACCT_NBR" as character varying(40)) as "GL_ACCT_NBR" ,
                    cast("GL_ACCT_NM" as character varying(120)) as "GL_ACCT_NM" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_TYPE_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("GL_ACCT_ALIAS_NM" as character varying(120)) as "GL_ACCT_ALIAS_NM" ,
                    cast("BLOCKED_IN_JOURNAL_IND" as NUMBER(38,0)) as "BLOCKED_IN_JOURNAL_IND" ,
                    cast("DPO_EXCLUSION_IND" as NUMBER(38,0)) as "DPO_EXCLUSION_IND" ,
                    cast("MOVEMENT_ALLOWED_IND" as NUMBER(38,0)) as "MOVEMENT_ALLOWED_IND" ,
                    cast("COST_CENTER_REQD_TXT" as character varying(16777216)) as "COST_CENTER_REQD_TXT" ,
                    cast("DEPT_REQD_TXT" as character varying(16777216)) as "DEPT_REQD_TXT" ,
                    cast("TYPE_REQD_TXT" as character varying(16777216)) as "TYPE_REQD_TXT" ,
                    cast("PRNT_GL_ACCT_NBR" as character varying(40)) as "PRNT_GL_ACCT_NBR" ,
                    cast("RELATED_GL_ACCT_NBR" as character varying(40)) as "RELATED_GL_ACCT_NBR" ,
                    cast("MONETARY_GL_ACCT_IND" as NUMBER(38,0)) as "MONETARY_GL_ACCT_IND" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BALANCE_SHEET_ACCT_IND" as NUMBER(1,0)) as "BALANCE_SHEET_ACCT_IND" ,
                    cast("SUMMARY_ACCT_IND" as NUMBER(1,0)) as "SUMMARY_ACCT_IND" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_chart_of_accts

            
        )

        
)

select * from v_chart_of_accts