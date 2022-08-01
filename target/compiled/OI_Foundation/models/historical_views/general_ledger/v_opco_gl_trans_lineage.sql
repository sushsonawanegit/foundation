with v_opco_gl_trans as(
    
    

        (
            select

                
                    cast("OPCO_GL_TRANS_SK" as character varying(32)) as "OPCO_GL_TRANS_SK" ,
                    cast("OPCO_GL_TRANS_CK" as character varying(32)) as "OPCO_GL_TRANS_CK" ,
                    cast("OPCO_GL_TRANS_AK" as character varying(16777216)) as "OPCO_GL_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PROJ_SK" as character varying(16777216)) as "OPCO_PROJ_SK" ,
                    cast("ORIGINAL_GL_OPCO_SK" as character varying(16777216)) as "ORIGINAL_GL_OPCO_SK" ,
                    cast("ORIGINAL_GL_VOUCHER_NBR" as character varying(16777216)) as "ORIGINAL_GL_VOUCHER_NBR" ,
                    cast("VOUCHER_NBR" as character varying(16777216)) as "VOUCHER_NBR" ,
                    cast("JOURNAL_BATCH_NBR" as character varying(16777216)) as "JOURNAL_BATCH_NBR" ,
                    cast("GL_DESC" as character varying(16777216)) as "GL_DESC" ,
                    cast("DOCUMENT_NBR" as character varying(16777216)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as DATE) as "DOCUMENT_DT" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(1,0)) as "CRRCTN_TRANS_IND" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("GL_QTY" as FLOAT) as "GL_QTY" ,
                    cast("TRANS_CURRENCY_GL_AMT" as FLOAT) as "TRANS_CURRENCY_GL_AMT" ,
                    cast("OPCO_CURRENCY_GL_AMT" as FLOAT) as "OPCO_CURRENCY_GL_AMT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("FSCL_MNTH_NBR" as NUMBER(38,0)) as "FSCL_MNTH_NBR" ,
                    cast("POSTED_IND" as NUMBER(1,0)) as "POSTED_IND" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" ,
                    cast(null as character varying(5)) as "SUB_DOCUMENT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_gl_trans

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_SK" as character varying(32)) as "OPCO_GL_TRANS_SK" ,
                    cast("OPCO_GL_TRANS_CK" as character varying(32)) as "OPCO_GL_TRANS_CK" ,
                    cast("OPCO_GL_TRANS_AK" as character varying(16777216)) as "OPCO_GL_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PROJ_SK" as character varying(16777216)) as "OPCO_PROJ_SK" ,
                    cast("ORIGINAL_GL_OPCO_SK" as character varying(16777216)) as "ORIGINAL_GL_OPCO_SK" ,
                    cast("ORIGINAL_GL_VOUCHER_NBR" as character varying(16777216)) as "ORIGINAL_GL_VOUCHER_NBR" ,
                    cast("VOUCHER_NBR" as character varying(16777216)) as "VOUCHER_NBR" ,
                    cast("JOURNAL_BATCH_NBR" as character varying(16777216)) as "JOURNAL_BATCH_NBR" ,
                    cast("GL_DESC" as character varying(16777216)) as "GL_DESC" ,
                    cast("DOCUMENT_NBR" as character varying(16777216)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as DATE) as "DOCUMENT_DT" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(1,0)) as "CRRCTN_TRANS_IND" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("GL_QTY" as FLOAT) as "GL_QTY" ,
                    cast("TRANS_CURRENCY_GL_AMT" as FLOAT) as "TRANS_CURRENCY_GL_AMT" ,
                    cast("OPCO_CURRENCY_GL_AMT" as FLOAT) as "OPCO_CURRENCY_GL_AMT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("FSCL_MNTH_NBR" as NUMBER(38,0)) as "FSCL_MNTH_NBR" ,
                    cast("POSTED_IND" as NUMBER(1,0)) as "POSTED_IND" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" ,
                    cast(null as character varying(5)) as "SUB_DOCUMENT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_gl_trans

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_SK" as character varying(32)) as "OPCO_GL_TRANS_SK" ,
                    cast("OPCO_GL_TRANS_CK" as character varying(32)) as "OPCO_GL_TRANS_CK" ,
                    cast("OPCO_GL_TRANS_AK" as character varying(16777216)) as "OPCO_GL_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PROJ_SK" as character varying(16777216)) as "OPCO_PROJ_SK" ,
                    cast("ORIGINAL_GL_OPCO_SK" as character varying(16777216)) as "ORIGINAL_GL_OPCO_SK" ,
                    cast("ORIGINAL_GL_VOUCHER_NBR" as character varying(16777216)) as "ORIGINAL_GL_VOUCHER_NBR" ,
                    cast("VOUCHER_NBR" as character varying(16777216)) as "VOUCHER_NBR" ,
                    cast("JOURNAL_BATCH_NBR" as character varying(16777216)) as "JOURNAL_BATCH_NBR" ,
                    cast("GL_DESC" as character varying(16777216)) as "GL_DESC" ,
                    cast("DOCUMENT_NBR" as character varying(16777216)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as DATE) as "DOCUMENT_DT" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(1,0)) as "CRRCTN_TRANS_IND" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("GL_QTY" as FLOAT) as "GL_QTY" ,
                    cast("TRANS_CURRENCY_GL_AMT" as FLOAT) as "TRANS_CURRENCY_GL_AMT" ,
                    cast("OPCO_CURRENCY_GL_AMT" as FLOAT) as "OPCO_CURRENCY_GL_AMT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("FSCL_MNTH_NBR" as NUMBER(38,0)) as "FSCL_MNTH_NBR" ,
                    cast("POSTED_IND" as NUMBER(1,0)) as "POSTED_IND" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" ,
                    cast("SUB_DOCUMENT_NBR" as character varying(5)) as "SUB_DOCUMENT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_gl_trans

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_GL_TRANS_SK" as character varying(32)) as "OPCO_GL_TRANS_SK" ,
                    cast("OPCO_GL_TRANS_CK" as character varying(32)) as "OPCO_GL_TRANS_CK" ,
                    cast("OPCO_GL_TRANS_AK" as character varying(16777216)) as "OPCO_GL_TRANS_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("TRANS_DT" as TIMESTAMP_NTZ) as "TRANS_DT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(32)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("OPCO_PROJ_SK" as character varying(16777216)) as "OPCO_PROJ_SK" ,
                    cast("ORIGINAL_GL_OPCO_SK" as character varying(16777216)) as "ORIGINAL_GL_OPCO_SK" ,
                    cast("ORIGINAL_GL_VOUCHER_NBR" as character varying(16777216)) as "ORIGINAL_GL_VOUCHER_NBR" ,
                    cast("VOUCHER_NBR" as character varying(16777216)) as "VOUCHER_NBR" ,
                    cast("JOURNAL_BATCH_NBR" as character varying(16777216)) as "JOURNAL_BATCH_NBR" ,
                    cast("GL_DESC" as character varying(16777216)) as "GL_DESC" ,
                    cast("DOCUMENT_NBR" as character varying(16777216)) as "DOCUMENT_NBR" ,
                    cast("DOCUMENT_DT" as DATE) as "DOCUMENT_DT" ,
                    cast("CRRCTN_TRANS_IND" as NUMBER(1,0)) as "CRRCTN_TRANS_IND" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("GL_QTY" as FLOAT) as "GL_QTY" ,
                    cast("TRANS_CURRENCY_GL_AMT" as FLOAT) as "TRANS_CURRENCY_GL_AMT" ,
                    cast("OPCO_CURRENCY_GL_AMT" as FLOAT) as "OPCO_CURRENCY_GL_AMT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("FSCL_MNTH_NBR" as NUMBER(38,0)) as "FSCL_MNTH_NBR" ,
                    cast("POSTED_IND" as NUMBER(1,0)) as "POSTED_IND" ,
                    cast("OPCO_UOM_SK" as character varying(32)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(32)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" ,
                    cast(null as character varying(5)) as "SUB_DOCUMENT_NBR" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_gl_trans

            
        )

        
)

select * from v_opco_gl_trans