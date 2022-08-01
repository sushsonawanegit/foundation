
  create or replace  view OI_DATA_DEV_V2.intermediate_fnd_bkp.v_opco_cust_trans_lineage 
  
   as (
    with v_opco_cust_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_CUST_TRANS_SK" as character varying(32)) as "OPCO_CUST_TRANS_SK" ,
                    cast("OPCO_CUST_TRANS_CK" as character varying(32)) as "OPCO_CUST_TRANS_CK" ,
                    cast("OPCO_CUST_TRANS_AK" as character varying(16777216)) as "OPCO_CUST_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CUST_SK" as character varying(32)) as "OPCO_CUST_SK" ,
                    cast("ORDER_CUST_SK" as character varying(32)) as "ORDER_CUST_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_SLS_ORDR_SK" as character varying(32)) as "OPCO_SLS_ORDR_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_PYMNT_TERMS_SK" as character varying(32)) as "OPCO_PYMNT_TERMS_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("OPCO_DLVRY_MODE_SK" as character varying(32)) as "OPCO_DLVRY_MODE_SK" ,
                    cast("OPCO_PROJECT_SK" as character varying(32)) as "OPCO_PROJECT_SK" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("TRANS_APPRVD_IND" as NUMBER(38,0)) as "TRANS_APPRVD_IND" ,
                    cast("VOUCHER_NBR" as character varying(40)) as "VOUCHER_NBR" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("OFFSET_SRC_KEY_TXT" as NUMBER(38,0)) as "OFFSET_SRC_KEY_TXT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(38,0)) as "CRRCTN_TRANS_IND" ,
                    cast("AUTO_SETTLE_IND" as NUMBER(38,0)) as "AUTO_SETTLE_IND" ,
                    cast("PYMNT_CANCELLED_IND" as NUMBER(38,0)) as "PYMNT_CANCELLED_IND" ,
                    cast("INTEREST_CALC_ALLOWED_IND" as NUMBER(38,0)) as "INTEREST_CALC_ALLOWED_IND" ,
                    cast("COLLECTION_LETTER_ALLOWED_IND" as NUMBER(38,0)) as "COLLECTION_LETTER_ALLOWED_IND" ,
                    cast("COD_CASH_PYMNT_IND" as NUMBER(38,0)) as "COD_CASH_PYMNT_IND" ,
                    cast("PYMNT_REF_TXT" as character varying(40)) as "PYMNT_REF_TXT" ,
                    cast("PYMNT_METHOD_TXT" as character varying(16777216)) as "PYMNT_METHOD_TXT" ,
                    cast("RETENTION_CD" as character varying(2)) as "RETENTION_CD" ,
                    cast("CHECK_RETENTION_IND" as NUMBER(38,0)) as "CHECK_RETENTION_IND" ,
                    cast("DOCUMENT_NBR" as character varying(40)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as TIMESTAMP_NTZ) as "DOCUMENT_DT" ,
                    cast("TRANS_DSCNT_DT" as TIMESTAMP_NTZ) as "TRANS_DSCNT_DT" ,
                    cast("LTST_INTEREST_DT" as TIMESTAMP_NTZ) as "LTST_INTEREST_DT" ,
                    cast("COLLECTION_LETTER_TXT" as character varying(16777216)) as "COLLECTION_LETTER_TXT" ,
                    cast("PO_FORM_NBR" as character varying(40)) as "PO_FORM_NBR" ,
                    cast("LTST_SETTLED_OPCO_SK" as character varying(8)) as "LTST_SETTLED_OPCO_SK" ,
                    cast("LTST_SETTLED_CUST_SK" as character varying(40)) as "LTST_SETTLED_CUST_SK" ,
                    cast("LTST_SETTLED_DT" as TIMESTAMP_NTZ) as "LTST_SETTLED_DT" ,
                    cast("LTST_SETTLED_VOUCHER_NBR" as character varying(40)) as "LTST_SETTLED_VOUCHER_NBR" ,
                    cast("TRANS_CLOSED_DT" as TIMESTAMP_NTZ) as "TRANS_CLOSED_DT" ,
                    cast("TRANS_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TRANS_AMT" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" ,
                    cast("TRANS_CURRENCY_TOTAL_SETTLED_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TOTAL_SETTLED_AMT" ,
                    cast("OPCO_CURRENCY_TOTAL_SETTLED_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TOTAL_SETTLED_AMT" ,
                    cast("TRANS_DSCNT_AMT" as NUMBER(28,12)) as "TRANS_DSCNT_AMT" ,
                    cast("SUM_TAX_AMT" as NUMBER(28,12)) as "SUM_TAX_AMT" ,
                    cast("TRANS_EXCH_RT" as NUMBER(28,12)) as "TRANS_EXCH_RT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_cust_trans

            
        )

        
)

select * from v_opco_cust_trans_lineage
  );
