
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_sls_ordr_line_lineage 
  
   as (
    with v_opco_sls_ordr_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SLS_ORDR_LINE_SK" as character varying(32)) as "OPCO_SLS_ORDR_LINE_SK" ,
                    cast("OPCO_SLS_ORDR_LINE_CK" as character varying(32)) as "OPCO_SLS_ORDR_LINE_CK" ,
                    cast("OPCO_SLS_ORDR_LINE_AK" as character varying(16777216)) as "OPCO_SLS_ORDR_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SLS_ORDR_ID" as character varying(40)) as "SLS_ORDR_ID" ,
                    cast("SLS_ORDR_LINE_NBR" as NUMBER(28,12)) as "SLS_ORDR_LINE_NBR" ,
                    cast("INVTRY_TRANS_ID" as character varying(40)) as "INVTRY_TRANS_ID" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_TRANS_STATUS_SK" as character varying(32)) as "OPCO_TRANS_STATUS_SK" ,
                    cast("OPCO_SLS_ORDR_TYPE_SK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("LINE_BLOCKED_IND" as NUMBER(38,0)) as "LINE_BLOCKED_IND" ,
                    cast("LINE_DLVRY_COMPLETE_IND" as NUMBER(38,0)) as "LINE_DLVRY_COMPLETE_IND" ,
                    cast("ITEM_NM" as character varying(2000)) as "ITEM_NM" ,
                    cast("DLVRY_TYPE_TXT" as character varying(16777216)) as "DLVRY_TYPE_TXT" ,
                    cast("PO_FORM_NBR" as character varying(40)) as "PO_FORM_NBR" ,
                    cast("DRAWING_LOCN_TXT" as character varying(200)) as "DRAWING_LOCN_TXT" ,
                    cast("ITEM_BOM_ID" as character varying(40)) as "ITEM_BOM_ID" ,
                    cast("DLVRY_DATE_CONTRL_TXT" as NUMBER(38,0)) as "DLVRY_DATE_CONTRL_TXT" ,
                    cast("SLS_ORDR_LINE_CRT_DTM" as TIMESTAMP_NTZ) as "SLS_ORDR_LINE_CRT_DTM" ,
                    cast("RQST_RCPT_DT" as TIMESTAMP_NTZ) as "RQST_RCPT_DT" ,
                    cast("CNFRM_RCPT_DT" as TIMESTAMP_NTZ) as "CNFRM_RCPT_DT" ,
                    cast("RQST_SHIP_DT" as TIMESTAMP_NTZ) as "RQST_SHIP_DT" ,
                    cast("CNFRM_SHIP_DT" as TIMESTAMP_NTZ) as "CNFRM_SHIP_DT" ,
                    cast("CNFRM_DLVRY_DT" as TIMESTAMP_NTZ) as "CNFRM_DLVRY_DT" ,
                    cast("GRP_NM" as character varying(30)) as "GRP_NM" ,
                    cast("TRANS_REF_ID" as character varying(40)) as "TRANS_REF_ID" ,
                    cast("MRKT_CD" as character varying(30)) as "MRKT_CD" ,
                    cast("SLS_ORDR_DTL_ID" as NUMBER(38,0)) as "SLS_ORDR_DTL_ID" ,
                    cast("PHASE_ID" as character varying(40)) as "PHASE_ID" ,
                    cast("PRIORITY_CD" as NUMBER(38,0)) as "PRIORITY_CD" ,
                    cast("RISK_IND" as NUMBER(38,0)) as "RISK_IND" ,
                    cast("SPCL_ITEM_IND" as NUMBER(38,0)) as "SPCL_ITEM_IND" ,
                    cast("SPCL_IN_SPCL_ITEM_IND" as NUMBER(38,0)) as "SPCL_IN_SPCL_ITEM_IND" ,
                    cast("SPCL_ITEM_ID" as character varying(24)) as "SPCL_ITEM_ID" ,
                    cast("TITAN_JOB_NBR" as character varying(40)) as "TITAN_JOB_NBR" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("TAX_ITEM_GRP_TXT" as character varying(20)) as "TAX_ITEM_GRP_TXT" ,
                    cast("SLS_GRP_TXT" as character varying(20)) as "SLS_GRP_TXT" ,
                    cast("SALES_UNIT_CD" as character varying(20)) as "SALES_UNIT_CD" ,
                    cast("ITEM_HEIGHT_NBR" as NUMBER(28,12)) as "ITEM_HEIGHT_NBR" ,
                    cast("ITEM_NET_WEIGHT_AMT" as NUMBER(28,12)) as "ITEM_NET_WEIGHT_AMT" ,
                    cast("INVTRY_QTY" as NUMBER(28,12)) as "INVTRY_QTY" ,
                    cast("ORDR_QTY" as NUMBER(28,12)) as "ORDR_QTY" ,
                    cast("NON_DLVRD_QTY" as NUMBER(28,12)) as "NON_DLVRD_QTY" ,
                    cast("NON_INVOICED_DLVRD_QTY" as NUMBER(28,12)) as "NON_INVOICED_DLVRD_QTY" ,
                    cast("SLS_QTY" as NUMBER(28,12)) as "SLS_QTY" ,
                    cast("SIS_QTY" as NUMBER(28,12)) as "SIS_QTY" ,
                    cast("UNIT_COST_PRICE_AMT" as NUMBER(28,12)) as "UNIT_COST_PRICE_AMT" ,
                    cast("UNIT_SLS_PRICE_AMT" as NUMBER(28,12)) as "UNIT_SLS_PRICE_AMT" ,
                    cast("ITEM_DEFAULT_PRICE_AMT" as NUMBER(28,12)) as "ITEM_DEFAULT_PRICE_AMT" ,
                    cast("ITEM_ONLINE_PRICE_AMT" as NUMBER(28,12)) as "ITEM_ONLINE_PRICE_AMT" ,
                    cast("ITEM_LIST_PRICE_AMT" as NUMBER(28,12)) as "ITEM_LIST_PRICE_AMT" ,
                    cast("ITEM_COST_AMT" as NUMBER(28,12)) as "ITEM_COST_AMT" ,
                    cast("TOTAL_SLS_AMT" as NUMBER(28,12)) as "TOTAL_SLS_AMT" ,
                    cast("FIXED_MISC_AMT" as NUMBER(28,12)) as "FIXED_MISC_AMT" ,
                    cast("IMBEDDED_FREIGHT_AMT" as NUMBER(28,12)) as "IMBEDDED_FREIGHT_AMT" ,
                    cast("IMBEDDED_FREIGHT_COST_AMT" as NUMBER(28,12)) as "IMBEDDED_FREIGHT_COST_AMT" ,
                    cast("OVERHEAD_COST_PCT" as NUMBER(28,12)) as "OVERHEAD_COST_PCT" ,
                    cast("LINE_DSCNT_PCT" as NUMBER(28,12)) as "LINE_DSCNT_PCT" ,
                    cast("ENGINEERING_HOURS_NBR" as NUMBER(28,12)) as "ENGINEERING_HOURS_NBR" ,
                    cast("LABOR_HOURS_NBR" as NUMBER(28,12)) as "LABOR_HOURS_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr_line

            
        )

        
)

select * from v_opco_sls_ordr_line_lineage
  );
