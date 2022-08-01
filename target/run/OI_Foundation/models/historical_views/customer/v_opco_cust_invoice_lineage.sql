
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_invoice_lineage 
  
   as (
    with v_opco_cust_invoice_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_INVOICE_SK" as character varying(32)) as "OPCO_CUST_INVOICE_SK" ,
                    cast("OPCO_CUST_INVOICE_CK" as character varying(32)) as "OPCO_CUST_INVOICE_CK" ,
                    cast("OPCO_CUST_INVOICE_AK" as character varying(16777216)) as "OPCO_CUST_INVOICE_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_PICKING_LIST_SK" as character varying(32)) as "OPCO_PICKING_LIST_SK" ,
                    cast("OPCO_CUST_PACKING_SLIP_SK" as character varying(32)) as "OPCO_CUST_PACKING_SLIP_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_SLS_ORDR_TYPE_SK" as character varying(32)) as "OPCO_SLS_ORDR_TYPE_SK" ,
                    cast("INVOICE_CUST_SK" as character varying(32)) as "INVOICE_CUST_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_CUST_GRP_SK" as character varying(32)) as "OPCO_CUST_GRP_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("OPCO_DLVRY_TERMS_SK" as character varying(32)) as "OPCO_DLVRY_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("DLVRY_LOCN_SK" as character varying(32)) as "DLVRY_LOCN_SK" ,
                    cast("INVOICE_LOCN_SK" as character varying(32)) as "INVOICE_LOCN_SK" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("TRANS_REF_TXT" as character varying(16777216)) as "TRANS_REF_TXT" ,
                    cast("INVOICE_DT" as TIMESTAMP_NTZ) as "INVOICE_DT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("CASH_DSCNT_DT" as TIMESTAMP_NTZ) as "CASH_DSCNT_DT" ,
                    cast("GL_VOUCHER_NBR" as character varying(40)) as "GL_VOUCHER_NBR" ,
                    cast("DOCUMENT_NBR" as character varying(40)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as TIMESTAMP_NTZ) as "DOCUMENT_DT" ,
                    cast("DLVRY_CUST_NM" as character varying(120)) as "DLVRY_CUST_NM" ,
                    cast("INVOICE_CUST_NM" as character varying(120)) as "INVOICE_CUST_NM" ,
                    cast("SLS_GRP_TXT" as character varying(20)) as "SLS_GRP_TXT" ,
                    cast("SLS_TAX_INCLUDED_IND" as NUMBER(38,0)) as "SLS_TAX_INCLUDED_IND" ,
                    cast("UPDT_AFTER_PRINTING_IND" as NUMBER(38,0)) as "UPDT_AFTER_PRINTING_IND" ,
                    cast("SPECIFIC_ACTION_TXT" as character varying(510)) as "SPECIFIC_ACTION_TXT" ,
                    cast("DISPATCH_LOCN_TXT" as character varying(20)) as "DISPATCH_LOCN_TXT" ,
                    cast("PYMNT_SCHED_CD" as character varying(60)) as "PYMNT_SCHED_CD" ,
                    cast("LEAD_CD" as character varying(30)) as "LEAD_CD" ,
                    cast("PO_FORM_NBR" as character varying(40)) as "PO_FORM_NBR" ,
                    cast("COMMSN_GRP_ID" as character varying(20)) as "COMMSN_GRP_ID" ,
                    cast("CREDIT_REASON_ID" as character varying(30)) as "CREDIT_REASON_ID" ,
                    cast("END_CUST_ID" as character varying(40)) as "END_CUST_ID" ,
                    cast("TAX_GRP_ID" as character varying(22)) as "TAX_GRP_ID" ,
                    cast("INVOICE_QTY" as NUMBER(28,12)) as "INVOICE_QTY" ,
                    cast("INVOICE_VOLUME_AMT" as NUMBER(28,12)) as "INVOICE_VOLUME_AMT" ,
                    cast("INVOICE_WEIGHT_AMT" as NUMBER(28,12)) as "INVOICE_WEIGHT_AMT" ,
                    cast("TRANS_CURRENCY_BALANCE_SLS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_BALANCE_SLS_AMT" ,
                    cast("OPCO_CURRENCY_BALANCE_SLS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_BALANCE_SLS_AMT" ,
                    cast("TRANS_CURRENCY_INVOICE_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_INVOICE_AMT" ,
                    cast("OPCO_CURRENCY_INVOICE_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_INVOICE_AMT" ,
                    cast("TRANS_CURRENCY_LINE_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_LINE_DSCNT_AMT" ,
                    cast("OPCO_CURRENCY_LINE_DSCNT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_LINE_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_CASH_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_CASH_DSCNT_AMT" ,
                    cast("TRANS_CURRENCY_SLS_TAX_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SLS_TAX_AMT" ,
                    cast("OPCO_CURRENCY_SLS_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SLS_TAX_AMT" ,
                    cast("TRANS_CURRENCY_MISC_CHRGS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_MISC_CHRGS_AMT" ,
                    cast("OPCO_CURRENTY_MISC_CHRGS_AMT" as NUMBER(28,12)) as "OPCO_CURRENTY_MISC_CHRGS_AMT" ,
                    cast("DLVRY_COST_AMT" as NUMBER(28,12)) as "DLVRY_COST_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_invoice

            
        )

        
)

select * from v_opco_cust_invoice_lineage
  );
