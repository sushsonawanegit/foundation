
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_empl_trans_lineage 
  
   as (
    with v_opco_project_empl_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_EMPL_TRANS_SK" as character varying(32)) as "OPCO_PROJECT_EMPL_TRANS_SK" ,
                    cast("OPCO_PROJECT_EMPL_TRANS_CK" as character varying(32)) as "OPCO_PROJECT_EMPL_TRANS_CK" ,
                    cast("OPCO_PROJECT_EMPL_TRANS_AK" as character varying(100)) as "OPCO_PROJECT_EMPL_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PROJECT_EMPL_TRANS_ID" as character varying(80)) as "PROJECT_EMPL_TRANS_ID" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PROJECT_TRANS_STATUS_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_STATUS_SK" ,
                    cast("OPCO_PROJECT_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("EMPL_ID" as character varying(80)) as "EMPL_ID" ,
                    cast("LINE_PROPERTY_ID" as character varying(80)) as "LINE_PROPERTY_ID" ,
                    cast("REF_PROJECT_EMPL_TRANS_ID" as character varying(80)) as "REF_PROJECT_EMPL_TRANS_ID" ,
                    cast("TAX_GRP_ID" as character varying(44)) as "TAX_GRP_ID" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("GL_VOUCHER_NBR" as character varying(80)) as "GL_VOUCHER_NBR" ,
                    cast("GL_SECTION_TXT" as character varying(16777216)) as "GL_SECTION_TXT" ,
                    cast("PROJECT_EMPL_TRANS_DESC" as character varying(240)) as "PROJECT_EMPL_TRANS_DESC" ,
                    cast("CALC_IND" as NUMBER(38,0)) as "CALC_IND" ,
                    cast("TRANS_HOUR_QTY" as NUMBER(28,12)) as "TRANS_HOUR_QTY" ,
                    cast("OPCO_CURRENCY_HRLY_COST_PRICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_HRLY_COST_PRICE_AMT" ,
                    cast("TRANS_CURRENCY_HRLY_SLS_PRICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_HRLY_SLS_PRICE_AMT" ,
                    cast("ITEM_PRICE_AMT" as NUMBER(28,12)) as "ITEM_PRICE_AMT" ,
                    cast("TOTAL_REVENUE_AMT" as NUMBER(28,12)) as "TOTAL_REVENUE_AMT" ,
                    cast("TOTAL_COST_AMT" as NUMBER(28,12)) as "TOTAL_COST_AMT" ,
                    cast("TOTAL_STD_COST_AMT" as NUMBER(28,12)) as "TOTAL_STD_COST_AMT" ,
                    cast("MARGIN_CONTRIBUTION_PCT" as NUMBER(28,12)) as "MARGIN_CONTRIBUTION_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_empl_trans

            
        )

        
)

select * from v_opco_project_empl_trans_lineage
  );
