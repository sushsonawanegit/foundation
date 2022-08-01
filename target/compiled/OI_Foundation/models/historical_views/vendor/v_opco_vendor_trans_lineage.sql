with v_opco_vendor_trans_lineage as(
    
    

        (
            select

                
                    cast("OPCO_VENDOR_TRANS_SK" as character varying(32)) as "OPCO_VENDOR_TRANS_SK" ,
                    cast("OPCO_VENDOR_TRANS_CK" as character varying(32)) as "OPCO_VENDOR_TRANS_CK" ,
                    cast("OPCO_VENDOR_TRANS_AK" as character varying(16777216)) as "OPCO_VENDOR_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(2)) as "SRC_SYS_NM" ,
                    cast("OPCO_ID" as character varying(8)) as "OPCO_ID" ,
                    cast("SRC_KEY_TXT" as NUMBER(38,0)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_VENDOR_SK" as character varying(32)) as "OPCO_VENDOR_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(32)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(32)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(32)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(32)) as "OPCO_LOB_SK" ,
                    cast("OPCO_PO_SK" as character varying(32)) as "OPCO_PO_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PYMNT_MODE_SK" as character varying(32)) as "OPCO_PYMNT_MODE_SK" ,
                    cast("OPCO_CASH_DSCNT_TERMS_SK" as character varying(32)) as "OPCO_CASH_DSCNT_TERMS_SK" ,
                    cast("LTST_SETTLED_OPCO_SK" as character varying(32)) as "LTST_SETTLED_OPCO_SK" ,
                    cast("LTST_SETTLED_VENDOR_SK" as character varying(32)) as "LTST_SETTLED_VENDOR_SK" ,
                    cast("TRANS_DESC" as character varying(120)) as "TRANS_DESC" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("DUE_DT" as TIMESTAMP_NTZ) as "DUE_DT" ,
                    cast("TRANS_CLOSE_DT" as TIMESTAMP_NTZ) as "TRANS_CLOSE_DT" ,
                    cast("VOUCHER_NBR" as character varying(40)) as "VOUCHER_NBR" ,
                    cast("INVOICE_NBR" as character varying(40)) as "INVOICE_NBR" ,
                    cast("LTST_SETTLED_VOUCHER_NBR" as character varying(40)) as "LTST_SETTLED_VOUCHER_NBR" ,
                    cast("LTST_SETTLE_DT" as TIMESTAMP_NTZ) as "LTST_SETTLE_DT" ,
                    cast("TRANS_APPRVD_IND" as NUMBER(38,0)) as "TRANS_APPRVD_IND" ,
                    cast("TRANS_APPRVD_BY_TXT" as character varying(40)) as "TRANS_APPRVD_BY_TXT" ,
                    cast("TRANS_APPRVD_DTM" as TIMESTAMP_NTZ) as "TRANS_APPRVD_DTM" ,
                    cast("DOCUMENT_NBR" as character varying(40)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as TIMESTAMP_NTZ) as "DOCUMENT_DT" ,
                    cast("OFFSET_SRC_KEY_TXT" as NUMBER(38,0)) as "OFFSET_SRC_KEY_TXT" ,
                    cast("PROJECT_CONTRACT_ORIGN_IND" as NUMBER(38,0)) as "PROJECT_CONTRACT_ORIGN_IND" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(38,0)) as "CRRCTN_TRANS_IND" ,
                    cast("AUTO_SETTLE_IND" as NUMBER(38,0)) as "AUTO_SETTLE_IND" ,
                    cast("CANCELLED_IND" as NUMBER(38,0)) as "CANCELLED_IND" ,
                    cast("PYMNT_REF_TXT" as character varying(40)) as "PYMNT_REF_TXT" ,
                    cast("PYMNT_ID" as character varying(400)) as "PYMNT_ID" ,
                    cast("TRANS_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_TRANS_AMT" ,
                    cast("TRANS_CURRENCY_SETTLED_AMT" as NUMBER(28,12)) as "TRANS_CURRENCY_SETTLED_AMT" ,
                    cast("OPCO_CURRENCY_TRANS_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TRANS_AMT" ,
                    cast("OPCO_CURRENCY_SETTLED_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SETTLED_AMT" ,
                    cast("EXCHG_RT" as NUMBER(28,12)) as "EXCHG_RT" ,
                    cast("LTST_EXCHG_ADJSTMT_DT" as TIMESTAMP_NTZ) as "LTST_EXCHG_ADJSTMT_DT" ,
                    cast("OPCO_CURRENCY_EXCHG_ADJSTMT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_EXCHG_ADJSTMT_AMT" ,
                    cast("OPCO_CURRENCY_REALIZED_EXCHG_ADJSTMT_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_REALIZED_EXCHG_ADJSTMT_AMT" ,
                    cast("OPCO_CURRENCY_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_TAX_AMT" ,
                    cast("OPCO_CURRENCY_SETTLED_TAX_AMT" as NUMBER(28,12)) as "OPCO_CURRENCY_SETTLED_TAX_AMT" ,
                    cast("DSCNT_DT" as TIMESTAMP_NTZ) as "DSCNT_DT" ,
                    cast("DSCNT_AMT" as NUMBER(28,12)) as "DSCNT_AMT" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_vendor_trans

            
        )

        
)

select * from v_opco_vendor_trans_lineage