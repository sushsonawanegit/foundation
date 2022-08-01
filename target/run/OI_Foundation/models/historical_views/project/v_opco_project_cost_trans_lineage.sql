
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_project_cost_trans_lineage 
  
   as (
    with v_opco_project_cost_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_PROJECT_COST_TRANS_SK" as character varying(32)) as "OPCO_PROJECT_COST_TRANS_SK" ,
                    cast("OPCO_PROJECT_COST_TRANS_CK" as character varying(32)) as "OPCO_PROJECT_COST_TRANS_CK" ,
                    cast("OPCO_PROJECT_COST_TRANS_AK" as character varying(100)) as "OPCO_PROJECT_COST_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(16)) as "OPCO_ID" ,
                    cast("PROJECT_COST_TRANS_ID" as character varying(80)) as "PROJECT_COST_TRANS_ID" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PROJECT_CATGRY_SK" as character varying(32)) as "OPCO_PROJECT_CATGRY_SK" ,
                    cast("OPCO_PROJECT_TRANS_STATUS_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_STATUS_SK" ,
                    cast("OPCO_PROJECT_TRANS_ORIGIN_SK" as character varying(32)) as "OPCO_PROJECT_TRANS_ORIGIN_SK" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("TRANS_DESC" as character varying(240)) as "TRANS_DESC" ,
                    cast("COST_GL_STATUS_TXT" as character varying(16777216)) as "COST_GL_STATUS_TXT" ,
                    cast("TAX_GRP_ID" as character varying(44)) as "TAX_GRP_ID" ,
                    cast("VOUCHER_NBR" as character varying(80)) as "VOUCHER_NBR" ,
                    cast("INVOICE_NBR" as character varying(80)) as "INVOICE_NBR" ,
                    cast("DOCUMENT_NBR" as character varying(80)) as "DOCUMENT_NBR" ,
                    cast("REF_TRANS_ID" as character varying(80)) as "REF_TRANS_ID" ,
                    cast("CALCULATED_IND" as NUMBER(38,0)) as "CALCULATED_IND" ,
                    cast("TRANS_QTY" as NUMBER(28,12)) as "TRANS_QTY" ,
                    cast("TRANS_CURRENCY_COST_PRICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_COST_PRICE_AMT" ,
                    cast("OPCO_CURRENCY_COST_PRICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_COST_PRICE_AMT" ,
                    cast("OPCO_CURRENCY_SLS_PRICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SLS_PRICE_AMT" ,
                    cast("OPCO_CURRENCY_GL_SLS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_GL_SLS_AMT" ,
                    cast("OPCO_CURRENCY_GL_COST_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_GL_COST_AMT" ,
                    cast("OPCO_CURRENCY_ITEM_PRICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_ITEM_PRICE_AMT" ,
                    cast("OPCO_CURRENCY_TOTAL_STD_COST_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TOTAL_STD_COST_AMT" ,
                    cast("OPCO_CURRENCY_TOTAL_COST_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TOTAL_COST_AMT" ,
                    cast("OPCO_CURRENCY_TOTAL_REVENUE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TOTAL_REVENUE_AMT" ,
                    cast("OPCO_CURRENCY_SLS_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SLS_TAX_AMT" ,
                    cast("OPCO_CURRENCY_FREIGHT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_FREIGHT_AMT" ,
                    cast("OPCO_CURRENCY_CASH_DSCNT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_CASH_DSCNT_AMT" ,
                    cast("MARGIN_CONTRIBUTION_PCT" as NUMBER(28,12)) as "MARGIN_CONTRIBUTION_PCT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_project_cost_trans

            
        )

        
)

select * from v_opco_project_cost_trans_lineage
  );
