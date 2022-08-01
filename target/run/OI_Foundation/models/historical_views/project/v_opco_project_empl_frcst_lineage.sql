
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_empl_frcst_lineage 
  
   as (
    with v_opco_project_empl_frcst_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_EMPL_FRCST_SK" as character varying(32)) as "OPCO_PROJECT_EMPL_FRCST_SK" ,
                    cast("OPCO_PROJECT_EMPL_FRCST_CK" as character varying(32)) as "OPCO_PROJECT_EMPL_FRCST_CK" ,
                    cast("OPCO_PROJECT_EMPL_FRCST_AK" as character varying(100)) as "OPCO_PROJECT_EMPL_FRCST_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PROJECT_EMPL_FRCST_ID" as character varying(80)) as "PROJECT_EMPL_FRCST_ID" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("LINE_NBR" as NUMBER(28,12)) as "LINE_NBR" ,
                    cast("LINE_PROPERTY_ID" as character varying(80)) as "LINE_PROPERTY_ID" ,
                    cast("EMPL_ID" as character varying(80)) as "EMPL_ID" ,
                    cast("MODEL_ID" as character varying(40)) as "MODEL_ID" ,
                    cast("TAX_GRP_ID" as character varying(44)) as "TAX_GRP_ID" ,
                    cast("INCLUDE_AMT_IND" as NUMBER(38,0)) as "INCLUDE_AMT_IND" ,
                    cast("LOCK_IND" as NUMBER(38,0)) as "LOCK_IND" ,
                    cast("SCHED_LINK_TYPE_TXT" as NUMBER(38,0)) as "SCHED_LINK_TYPE_TXT" ,
                    cast("SCHED_LINK_TXT" as NUMBER(38,0)) as "SCHED_LINK_TXT" ,
                    cast("SCHED_FROM_DT" as TIMESTAMP_NTZ) as "SCHED_FROM_DT" ,
                    cast("SCHED_WORKTIME_IND" as NUMBER(38,0)) as "SCHED_WORKTIME_IND" ,
                    cast("SCHED_CAPACITY_IND" as NUMBER(38,0)) as "SCHED_CAPACITY_IND" ,
                    cast("REVENUE_PYMNT_EXPCTD_DT" as TIMESTAMP_NTZ) as "REVENUE_PYMNT_EXPCTD_DT" ,
                    cast("COST_PYMNT_EXPCTD_DT" as TIMESTAMP_NTZ) as "COST_PYMNT_EXPCTD_DT" ,
                    cast("INVOICE_EXPCTD_DT" as TIMESTAMP_NTZ) as "INVOICE_EXPCTD_DT" ,
                    cast("ELIMINATIN_EXPCTD_DT" as TIMESTAMP_NTZ) as "ELIMINATIN_EXPCTD_DT" ,
                    cast("TRANS_HOURS_QTY" as NUMBER(28,12)) as "TRANS_HOURS_QTY" ,
                    cast("SCHED_LOAD_PCT" as NUMBER(28,12)) as "SCHED_LOAD_PCT" ,
                    cast("TRANS_CURRENCY_COST_PRICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_COST_PRICE_AMT" ,
                    cast("TRANS_CURRENCY_SLS_PRICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SLS_PRICE_AMT" ,
                    cast("TOTAL_REVENUE_AMT" as NUMBER(28,12)) as "TOTAL_REVENUE_AMT" ,
                    cast("TOTAL_COST_AMT" as NUMBER(28,12)) as "TOTAL_COST_AMT" ,
                    cast("MARGIN_CONTRIBUTION_PCT" as NUMBER(28,12)) as "MARGIN_CONTRIBUTION_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_empl_frcst

            
        )

        
)

select * from v_opco_project_empl_frcst_lineage
  );
