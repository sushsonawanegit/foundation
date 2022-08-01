
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_invoice_line_lineage 
  
   as (
    with v_opco_cust_invoice_line_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_INVOICE_LINE_SK" as character varying(32)) as "OPCO_CUST_INVOICE_LINE_SK" ,
                    cast("OPCO_CUST_INVOICE_LINE_CK" as character varying(32)) as "OPCO_CUST_INVOICE_LINE_CK" ,
                    cast("OPCO_CUST_INVOICE_LINE_AK" as character varying(16777216)) as "OPCO_CUST_INVOICE_LINE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("ORIG_SLS_ORDR_SK" as character varying(32)) as "ORIG_SLS_ORDR_SK" ,
                    cast("OPCO_ITEM_SK" as character varying(32)) as "OPCO_ITEM_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("SLS_UOM_SK" as character varying(32)) as "SLS_UOM_SK" ,
                    cast("GL_PROD_UOM_SK" as character varying(32)) as "GL_PROD_UOM_SK" ,
                    cast("GL_SLS_UOM_SK" as character varying(32)) as "GL_SLS_UOM_SK" ,
                    cast("OPCO_ASSCTN_SK" as character varying(32)) as "OPCO_ASSCTN_SK" ,
                    cast("OPCO_INVTRY_REF_TYPE_SK" as character varying(32)) as "OPCO_INVTRY_REF_TYPE_SK" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("LINE_NBR" as NUMBER(28,12)) as "LINE_NBR" ,
                    cast("INVTRY_TRANS_ID" as character varying(40)) as "INVTRY_TRANS_ID" ,
                    cast("CUST_ITEM_ID" as character varying(40)) as "CUST_ITEM_ID" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("TAX_ITEM_GRP_ID" as character varying(20)) as "TAX_ITEM_GRP_ID" ,
                    cast("INVTRY_REF_ID" as character varying(40)) as "INVTRY_REF_ID" ,
                    cast("ITEM_BOM_ID" as character varying(40)) as "ITEM_BOM_ID" ,
                    cast("ITEM_DESC" as character varying(2000)) as "ITEM_DESC" ,
                    cast("SLS_GRP_TXT" as character varying(20)) as "SLS_GRP_TXT" ,
                    cast("SHPMT_COUNTRY_NM" as character varying(20)) as "SHPMT_COUNTRY_NM" ,
                    cast("DLVRY_COUNTRY_NM" as character varying(20)) as "DLVRY_COUNTRY_NM" ,
                    cast("DLVRY_STATE_NM" as character varying(20)) as "DLVRY_STATE_NM" ,
                    cast("DLVRY_COUNTY_NM" as character varying(60)) as "DLVRY_COUNTY_NM" ,
                    cast("MARK_TXT" as character varying(30)) as "MARK_TXT" ,
                    cast("GRP_NM" as character varying(30)) as "GRP_NM" ,
                    cast("SHIP_DT" as TIMESTAMP_NTZ) as "SHIP_DT" ,
                    cast("PARTIAL_DLVRY_IND" as NUMBER(38,0)) as "PARTIAL_DLVRY_IND" ,
                    cast("RISK_TYPE_IND" as NUMBER(38,0)) as "RISK_TYPE_IND" ,
                    cast("KITS_IND" as NUMBER(38,0)) as "KITS_IND" ,
                    cast("COMMSN_CALC_IND" as NUMBER(38,0)) as "COMMSN_CALC_IND" ,
                    cast("DLVRY_TYPE_TXT" as character varying(16777216)) as "DLVRY_TYPE_TXT" ,
                    cast("INVOICE_LINE_TXT" as character varying(160)) as "INVOICE_LINE_TXT" ,
                    cast("SPCL_ITEM_IND" as NUMBER(38,0)) as "SPCL_ITEM_IND" ,
                    cast("SPCL_IN_SPCL_IND" as NUMBER(38,0)) as "SPCL_IN_SPCL_IND" ,
                    cast("SPCL_ITEM_NBR" as character varying(24)) as "SPCL_ITEM_NBR" ,
                    cast("LABOR_HOURS_NBR" as NUMBER(28,12)) as "LABOR_HOURS_NBR" ,
                    cast("ENGINEERING_HOURS_NBR" as NUMBER(28,12)) as "ENGINEERING_HOURS_NBR" ,
                    cast("ITEM_WEIGHT_AMT" as NUMBER(28,12)) as "ITEM_WEIGHT_AMT" ,
                    cast("PER_UNIT_QTY" as NUMBER(28,12)) as "PER_UNIT_QTY" ,
                    cast("INVOICE_ITEM_QTY" as NUMBER(28,12)) as "INVOICE_ITEM_QTY" ,
                    cast("ORDR_QTY" as NUMBER(28,12)) as "ORDR_QTY" ,
                    cast("DLVRD_QTY" as NUMBER(28,12)) as "DLVRD_QTY" ,
                    cast("PREV_INVOICED_QTY" as NUMBER(28,12)) as "PREV_INVOICED_QTY" ,
                    cast("REMAINING_QTY" as NUMBER(28,12)) as "REMAINING_QTY" ,
                    cast("INVTRY_UNITS_QTY" as NUMBER(28,12)) as "INVTRY_UNITS_QTY" ,
                    cast("GL_PROD_QTY" as NUMBER(28,12)) as "GL_PROD_QTY" ,
                    cast("GL_SLS_QTY" as NUMBER(28,12)) as "GL_SLS_QTY" ,
                    cast("SIS_QTY" as NUMBER(28,12)) as "SIS_QTY" ,
                    cast("DSCNT_PCT" as NUMBER(28,12)) as "DSCNT_PCT" ,
                    cast("LINE_DSCNT_PCT" as NUMBER(28,12)) as "LINE_DSCNT_PCT" ,
                    cast("OVERHEAD_COST_PCT" as NUMBER(28,12)) as "OVERHEAD_COST_PCT" ,
                    cast("MARGIN_CONTRIBUTION_PCT" as NUMBER(28,12)) as "MARGIN_CONTRIBUTION_PCT" ,
                    cast("TRANS_CURRENCY_SLS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SLS_AMT" ,
                    cast("OPCO_CURRENCY_SLS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SLS_AMT" ,
                    cast("TRANS_CURRENCY_INCL_SLS_TAX_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_INCL_SLS_TAX_AMT" ,
                    cast("OPCO_CURRENCY_INCL_SLS_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_INCL_SLS_TAX_AMT" ,
                    cast("TRANS_CURRENCY_SUM_LINE_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SUM_LINE_DSCNT_AMT" ,
                    cast("OPCO_CURRENCY_SUM_LINE_DSCNT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SUM_LINE_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_TAX_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TAX_AMT" ,
                    cast("OPCO_CURRENCY_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TAX_AMT" ,
                    cast("TRANS_CURRENCY_COMMSN_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_COMMSN_AMT" ,
                    cast("OPCO_CURRENCY_COMMSN_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_COMMSN_AMT" ,
                    cast("SLS_MARKUP_AMT" as NUMBER(28,12)) as "SLS_MARKUP_AMT" ,
                    cast("PER_UNIT_SLS_PRICE_AMT" as NUMBER(28,12)) as "PER_UNIT_SLS_PRICE_AMT" ,
                    cast("ONLINE_PRICE_AMT" as NUMBER(28,12)) as "ONLINE_PRICE_AMT" ,
                    cast("LIST_PRICE_AMT" as NUMBER(28,12)) as "LIST_PRICE_AMT" ,
                    cast("IMBEDDED_FREIGHT_COST_AMT" as NUMBER(28,12)) as "IMBEDDED_FREIGHT_COST_AMT" ,
                    cast("IMBEDDED_FREIGHT_AMT" as NUMBER(28,12)) as "IMBEDDED_FREIGHT_AMT" ,
                    cast("TOTAL_COST_AMT" as NUMBER(28,12)) as "TOTAL_COST_AMT" ,
                    cast("ITEM_COST_AMT" as NUMBER(28,12)) as "ITEM_COST_AMT" ,
                    cast("DEFAULT_ITEM_PRICE_AMT" as NUMBER(28,12)) as "DEFAULT_ITEM_PRICE_AMT" ,
                    cast("ITEM_PRICE_AMT" as NUMBER(28,12)) as "ITEM_PRICE_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_invoice_line

            
        )

        
)

select * from v_opco_cust_invoice_line_lineage
  );
