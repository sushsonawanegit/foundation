
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_chart_of_accts_lineage 
  
   as (
    with v_opco_chart_of_accts as(
    
    

        (
            select

                
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_CK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("GL_ACCT_NBR" as character varying(105)) as "GL_ACCT_NBR" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("GL_ACCT_NM" as character varying(100)) as "GL_ACCT_NM" ,
                    cast("CHART_OF_ACCTS_SK" as character varying(32)) as "CHART_OF_ACCTS_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_TYPE_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("GL_ACCT_ALIAS_NM" as character varying(16777216)) as "GL_ACCT_ALIAS_NM" ,
                    cast("MANUAL_ENTRY_ALLOWED_IND" as NUMBER(1,0)) as "MANUAL_ENTRY_ALLOWED_IND" ,
                    cast("DPO_EXCLUSION_IND" as NUMBER(1,0)) as "DPO_EXCLUSION_IND" ,
                    cast("MOVEMENT_ALLOWED_IND" as NUMBER(1,0)) as "MOVEMENT_ALLOWED_IND" ,
                    cast("COST_CENTER_REQD_TXT" as character varying(16777216)) as "COST_CENTER_REQD_TXT" ,
                    cast("DEPT_REQD_TXT" as character varying(16777216)) as "DEPT_REQD_TXT" ,
                    cast("TYPE_REQD_TXT" as character varying(16777216)) as "TYPE_REQD_TXT" ,
                    cast("PRNT_GL_ACCT_NBR" as character varying(16777216)) as "PRNT_GL_ACCT_NBR" ,
                    cast("RELATED_GL_ACCT_NBR" as character varying(16777216)) as "RELATED_GL_ACCT_NBR" ,
                    cast("MONETARY_GL_ACCT_IND" as character varying(16777216)) as "MONETARY_GL_ACCT_IND" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BALANCE_SHEET_ACCT_IND" as NUMBER(1,0)) as "BALANCE_SHEET_ACCT_IND" ,
                    cast("SUMMARY_ACCT_IND" as NUMBER(1,0)) as "SUMMARY_ACCT_IND" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("ACCT_CLSSFCTN_CD" as character varying(16777216)) as "ACCT_CLSSFCTN_CD" ,
                    cast("SUB_ACCT_NBR" as character varying(16777216)) as "SUB_ACCT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_chart_of_accts

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_CK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("GL_ACCT_NBR" as character varying(105)) as "GL_ACCT_NBR" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("GL_ACCT_NM" as character varying(100)) as "GL_ACCT_NM" ,
                    cast("CHART_OF_ACCTS_SK" as character varying(32)) as "CHART_OF_ACCTS_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_TYPE_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("GL_ACCT_ALIAS_NM" as character varying(16777216)) as "GL_ACCT_ALIAS_NM" ,
                    cast("MANUAL_ENTRY_ALLOWED_IND" as NUMBER(1,0)) as "MANUAL_ENTRY_ALLOWED_IND" ,
                    cast("DPO_EXCLUSION_IND" as NUMBER(1,0)) as "DPO_EXCLUSION_IND" ,
                    cast("MOVEMENT_ALLOWED_IND" as NUMBER(1,0)) as "MOVEMENT_ALLOWED_IND" ,
                    cast("COST_CENTER_REQD_TXT" as character varying(16777216)) as "COST_CENTER_REQD_TXT" ,
                    cast("DEPT_REQD_TXT" as character varying(16777216)) as "DEPT_REQD_TXT" ,
                    cast("TYPE_REQD_TXT" as character varying(16777216)) as "TYPE_REQD_TXT" ,
                    cast("PRNT_GL_ACCT_NBR" as character varying(16777216)) as "PRNT_GL_ACCT_NBR" ,
                    cast("RELATED_GL_ACCT_NBR" as character varying(16777216)) as "RELATED_GL_ACCT_NBR" ,
                    cast("MONETARY_GL_ACCT_IND" as character varying(16777216)) as "MONETARY_GL_ACCT_IND" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BALANCE_SHEET_ACCT_IND" as NUMBER(1,0)) as "BALANCE_SHEET_ACCT_IND" ,
                    cast("SUMMARY_ACCT_IND" as NUMBER(1,0)) as "SUMMARY_ACCT_IND" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("ACCT_CLSSFCTN_CD" as character varying(16777216)) as "ACCT_CLSSFCTN_CD" ,
                    cast("SUB_ACCT_NBR" as character varying(16777216)) as "SUB_ACCT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_chart_of_accts

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_CK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("GL_ACCT_NBR" as character varying(105)) as "GL_ACCT_NBR" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("GL_ACCT_NM" as character varying(100)) as "GL_ACCT_NM" ,
                    cast("CHART_OF_ACCTS_SK" as character varying(32)) as "CHART_OF_ACCTS_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_TYPE_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("GL_ACCT_ALIAS_NM" as character varying(16777216)) as "GL_ACCT_ALIAS_NM" ,
                    cast("MANUAL_ENTRY_ALLOWED_IND" as NUMBER(1,0)) as "MANUAL_ENTRY_ALLOWED_IND" ,
                    cast("DPO_EXCLUSION_IND" as NUMBER(1,0)) as "DPO_EXCLUSION_IND" ,
                    cast("MOVEMENT_ALLOWED_IND" as NUMBER(1,0)) as "MOVEMENT_ALLOWED_IND" ,
                    cast("COST_CENTER_REQD_TXT" as character varying(16777216)) as "COST_CENTER_REQD_TXT" ,
                    cast("DEPT_REQD_TXT" as character varying(16777216)) as "DEPT_REQD_TXT" ,
                    cast("TYPE_REQD_TXT" as character varying(16777216)) as "TYPE_REQD_TXT" ,
                    cast("PRNT_GL_ACCT_NBR" as character varying(16777216)) as "PRNT_GL_ACCT_NBR" ,
                    cast("RELATED_GL_ACCT_NBR" as character varying(16777216)) as "RELATED_GL_ACCT_NBR" ,
                    cast("MONETARY_GL_ACCT_IND" as character varying(16777216)) as "MONETARY_GL_ACCT_IND" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BALANCE_SHEET_ACCT_IND" as NUMBER(1,0)) as "BALANCE_SHEET_ACCT_IND" ,
                    cast("SUMMARY_ACCT_IND" as NUMBER(1,0)) as "SUMMARY_ACCT_IND" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("ACCT_CLSSFCTN_CD" as character varying(16777216)) as "ACCT_CLSSFCTN_CD" ,
                    cast("SUB_ACCT_NBR" as character varying(16777216)) as "SUB_ACCT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_chart_of_accts

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_CK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_CK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("GL_ACCT_NBR" as character varying(105)) as "GL_ACCT_NBR" ,
                    cast("OPCO_ID" as character varying(15)) as "OPCO_ID" ,
                    cast("GL_ACCT_NM" as character varying(100)) as "GL_ACCT_NM" ,
                    cast("CHART_OF_ACCTS_SK" as character varying(32)) as "CHART_OF_ACCTS_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_TYPE_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_TYPE_SK" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("GL_ACCT_ALIAS_NM" as character varying(16777216)) as "GL_ACCT_ALIAS_NM" ,
                    cast("MANUAL_ENTRY_ALLOWED_IND" as NUMBER(1,0)) as "MANUAL_ENTRY_ALLOWED_IND" ,
                    cast("DPO_EXCLUSION_IND" as NUMBER(1,0)) as "DPO_EXCLUSION_IND" ,
                    cast("MOVEMENT_ALLOWED_IND" as NUMBER(1,0)) as "MOVEMENT_ALLOWED_IND" ,
                    cast("COST_CENTER_REQD_TXT" as character varying(16777216)) as "COST_CENTER_REQD_TXT" ,
                    cast("DEPT_REQD_TXT" as character varying(16777216)) as "DEPT_REQD_TXT" ,
                    cast("TYPE_REQD_TXT" as character varying(16777216)) as "TYPE_REQD_TXT" ,
                    cast("PRNT_GL_ACCT_NBR" as character varying(16777216)) as "PRNT_GL_ACCT_NBR" ,
                    cast("RELATED_GL_ACCT_NBR" as character varying(16777216)) as "RELATED_GL_ACCT_NBR" ,
                    cast("MONETARY_GL_ACCT_IND" as character varying(16777216)) as "MONETARY_GL_ACCT_IND" ,
                    cast("ACTV_IND" as NUMBER(1,0)) as "ACTV_IND" ,
                    cast("BALANCE_SHEET_ACCT_IND" as NUMBER(1,0)) as "BALANCE_SHEET_ACCT_IND" ,
                    cast("SUMMARY_ACCT_IND" as NUMBER(1,0)) as "SUMMARY_ACCT_IND" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("ACCT_CLSSFCTN_CD" as character varying(16777216)) as "ACCT_CLSSFCTN_CD" ,
                    cast("SUB_ACCT_NBR" as character varying(16777216)) as "SUB_ACCT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_chart_of_accts

            
        )

        
)

select * from v_opco_chart_of_accts
  );
