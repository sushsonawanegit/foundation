with v_opco_sls_ordr_lineage as(
    
    

        (
            select

                
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_SLS_ORDR_CK" as character varying(32)) as "OPCO_SLS_ORDR_CK" ,
                    cast("OPCO_SLS_ORDR_AK" as character varying(52)) as "OPCO_SLS_ORDR_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SLS_ORDR_ID" as character varying(40)) as "SLS_ORDR_ID" ,
                    cast("SLS_ORDR_NM" as character varying(120)) as "SLS_ORDR_NM" ,
                    cast("SLS_ORDR_DT" as TIMESTAMP_NTZ) as "SLS_ORDR_DT" ,
                    cast("SHIP_DT" as TIMESTAMP_NTZ) as "SHIP_DT" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_TRANS_STATUS_SK" as character varying(32)) as "OPCO_TRANS_STATUS_SK" ,
                    cast("OPCO_CUST_GRP_SK" as character varying(32)) as "OPCO_CUST_GRP_SK" ,
                    cast("OPCO_SITE_SK" as character varying(32)) as "OPCO_SITE_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("DLVRY_LOC_SK" as character varying(32)) as "DLVRY_LOC_SK" ,
                    cast("WAREHOUSE_SK" as character varying(32)) as "WAREHOUSE_SK" ,
                    cast("SLS_GRP_NM" as character varying(20)) as "SLS_GRP_NM" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("QUOTATION_ID" as character varying(40)) as "QUOTATION_ID" ,
                    cast("CUST_PO_NBR" as character varying(40)) as "CUST_PO_NBR" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("CASH_DSCNT_PCT" as NUMBER(28,12)) as "CASH_DSCNT_PCT" ,
                    cast("SLS_ORDR_CRT_DTM" as TIMESTAMP_NTZ) as "SLS_ORDR_CRT_DTM" ,
                    cast("SLS_ORDR_MODIFIED_DTM" as TIMESTAMP_NTZ) as "SLS_ORDR_MODIFIED_DTM" ,
                    cast("SLS_ORDR_CRT_BY_NM" as character varying(10)) as "SLS_ORDR_CRT_BY_NM" ,
                    cast("SLS_ORDR_MODIFIED_BY_NM" as character varying(10)) as "SLS_ORDR_MODIFIED_BY_NM" ,
                    cast("ON_HOLD_IND" as NUMBER(38,0)) as "ON_HOLD_IND" ,
                    cast("ON_HOLD_BY_NM" as character varying(20)) as "ON_HOLD_BY_NM" ,
                    cast("ON_HOLD_RSN_DESC" as character varying(60)) as "ON_HOLD_RSN_DESC" ,
                    cast("DSCNT_PCT" as NUMBER(28,12)) as "DSCNT_PCT" ,
                    cast("END_CUST_NM" as character varying(40)) as "END_CUST_NM" ,
                    cast("CUST_REF_TXT" as character varying(70)) as "CUST_REF_TXT" ,
                    cast("DLVRY_CUST_NM" as character varying(120)) as "DLVRY_CUST_NM" ,
                    cast("OPCO_SLS_ORDR_DOC_STATUS_SK" as character varying(32)) as "OPCO_SLS_ORDR_DOC_STATUS_SK" ,
                    cast("INVOICE_CUST_SK" as character varying(32)) as "INVOICE_CUST_SK" ,
                    cast("LINE_DSCNT_CD" as character varying(20)) as "LINE_DSCNT_CD" ,
                    cast("SLS_ORDR_PHASE_CD" as character varying(40)) as "SLS_ORDR_PHASE_CD" ,
                    cast("TRADE_GRP_CD" as character varying(20)) as "TRADE_GRP_CD" ,
                    cast("TRADE_GRP_NM" as character varying(1)) as "TRADE_GRP_NM" ,
                    cast("TRADE_GRP_MODULE_TXT" as character varying(1)) as "TRADE_GRP_MODULE_TXT" ,
                    cast("TRADE_GRP_TYPE_TXT" as character varying(1)) as "TRADE_GRP_TYPE_TXT" ,
                    cast("RCPT_CNFRM_DT" as TIMESTAMP_NTZ) as "RCPT_CNFRM_DT" ,
                    cast("RCPT_RQST_DT" as TIMESTAMP_NTZ) as "RCPT_RQST_DT" ,
                    cast("SLS_TAKER_NM" as character varying(40)) as "SLS_TAKER_NM" ,
                    cast("OPCO_SLS_ORDR_TYPE_SK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_SK" ,
                    cast("SHIP_CNFRM_DT" as TIMESTAMP_NTZ) as "SHIP_CNFRM_DT" ,
                    cast("SHIP_RQST_DT" as TIMESTAMP_NTZ) as "SHIP_RQST_DT" ,
                    cast("TITAN_JOB_NBR" as character varying(40)) as "TITAN_JOB_NBR" ,
                    cast("ONLINE_ORDR_IND" as NUMBER(38,0)) as "ONLINE_ORDR_IND" ,
                    cast("SPCL_INSTR_TXT" as character varying(1000)) as "SPCL_INSTR_TXT" ,
                    cast("TAX_EXEMPT_NBR" as character varying(40)) as "TAX_EXEMPT_NBR" ,
                    cast("COMMSN_GRP_ID" as character varying(20)) as "COMMSN_GRP_ID" ,
                    cast("COMMSN_GRP_NM" as character varying(120)) as "COMMSN_GRP_NM" ,
                    cast("LAST_ACCEPTANCE_DT" as TIMESTAMP_NTZ) as "LAST_ACCEPTANCE_DT" ,
                    cast("FIXED_DUE_DT" as TIMESTAMP_NTZ) as "FIXED_DUE_DT" ,
                    cast("BACKLOG_COST_AMT" as NUMBER(28,12)) as "BACKLOG_COST_AMT" ,
                    cast("BACKLOG_SLS_AMT" as NUMBER(28,12)) as "BACKLOG_SLS_AMT" ,
                    cast("BID_DT" as TIMESTAMP_NTZ) as "BID_DT" ,
                    cast("PRINT_INVOICE_IND" as NUMBER(1,0)) as "PRINT_INVOICE_IND" ,
                    cast("SLS_LEAD_CD" as character varying(30)) as "SLS_LEAD_CD" ,
                    cast("CUST_ALT_NM" as character varying(40)) as "CUST_ALT_NM" ,
                    cast("DSPTCH_LOCN_NM" as character varying(20)) as "DSPTCH_LOCN_NM" ,
                    cast("EQUIPMENT_TYPE_TXT" as character varying(40)) as "EQUIPMENT_TYPE_TXT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_sls_ordr

            
        )

        
)

select * from v_opco_sls_ordr_lineage