with v_opco_fscl_yr_gl_opening_bal as(
    
    

        (
            select

                
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_SK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_SK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_CK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_CK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_AK" as character varying(16777216)) as "OPCO_FSCL_YR_GL_OPENING_BAL_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("FSCL_YR_STRT_DT" as DATE) as "FSCL_YR_STRT_DT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("OPENING_BAL_QTY" as FLOAT) as "OPENING_BAL_QTY" ,
                    cast("TRANS_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "TRANS_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "OPCO_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast(null as character varying(16777216)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast(null as character varying(16777216)) as "SUB_LEDGER_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ax_opco_fscl_yr_gl_opening_bal

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_SK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_SK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_CK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_CK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_AK" as character varying(16777216)) as "OPCO_FSCL_YR_GL_OPENING_BAL_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("FSCL_YR_STRT_DT" as DATE) as "FSCL_YR_STRT_DT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("OPENING_BAL_QTY" as FLOAT) as "OPENING_BAL_QTY" ,
                    cast("TRANS_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "TRANS_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "OPCO_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast(null as character varying(16777216)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast(null as character varying(16777216)) as "SUB_LEDGER_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.ns_opco_fscl_yr_gl_opening_bal

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_SK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_SK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_CK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_CK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_AK" as character varying(16777216)) as "OPCO_FSCL_YR_GL_OPENING_BAL_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("FSCL_YR_STRT_DT" as DATE) as "FSCL_YR_STRT_DT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("OPENING_BAL_QTY" as FLOAT) as "OPENING_BAL_QTY" ,
                    cast("TRANS_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "TRANS_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "OPCO_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(16777216)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.jde_opco_fscl_yr_gl_opening_bal

            
        )

        union all
        

        (
            select

                
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_SK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_SK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_CK" as character varying(32)) as "OPCO_FSCL_YR_GL_OPENING_BAL_CK" ,
                    cast("OPCO_FSCL_YR_GL_OPENING_BAL_AK" as character varying(16777216)) as "OPCO_FSCL_YR_GL_OPENING_BAL_AK" ,
                    cast("CRT_DTM" as TIMESTAMP_LTZ) as "CRT_DTM" ,
                    cast("STG_LOAD_DTM" as TIMESTAMP_TZ) as "STG_LOAD_DTM" ,
                    cast("DELETE_DTM" as TIMESTAMP_TZ) as "DELETE_DTM" ,
                    cast("SRC_SYS_NM" as character varying(20)) as "SRC_SYS_NM" ,
                    cast("SRC_KEY_TXT" as character varying(16777216)) as "SRC_KEY_TXT" ,
                    cast("OPCO_SK" as character varying(32)) as "OPCO_SK" ,
                    cast("OPCO_CHART_OF_ACCTS_SK" as character varying(32)) as "OPCO_CHART_OF_ACCTS_SK" ,
                    cast("OPCO_GL_TRANS_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_TYPE_SK" ,
                    cast("OPCO_GL_TRANS_POSTING_TYPE_SK" as character varying(16777216)) as "OPCO_GL_TRANS_POSTING_TYPE_SK" ,
                    cast("OPCO_COST_CENTER_SK" as character varying(32)) as "OPCO_COST_CENTER_SK" ,
                    cast("OPCO_DEPT_SK" as character varying(16777216)) as "OPCO_DEPT_SK" ,
                    cast("OPCO_TYPE_SK" as character varying(16777216)) as "OPCO_TYPE_SK" ,
                    cast("OPCO_PURPOSE_SK" as character varying(16777216)) as "OPCO_PURPOSE_SK" ,
                    cast("OPCO_LOB_SK" as character varying(16777216)) as "OPCO_LOB_SK" ,
                    cast("TRANS_CURRENCY_SK" as character varying(32)) as "TRANS_CURRENCY_SK" ,
                    cast("FSCL_YR_STRT_DT" as DATE) as "FSCL_YR_STRT_DT" ,
                    cast("FSCL_YR_NBR" as NUMBER(38,0)) as "FSCL_YR_NBR" ,
                    cast("CREDIT_IND" as NUMBER(1,0)) as "CREDIT_IND" ,
                    cast("OPENING_BAL_QTY" as FLOAT) as "OPENING_BAL_QTY" ,
                    cast("TRANS_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "TRANS_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_CURRENCY_OPENING_BAL_AMT" as FLOAT) as "OPCO_CURRENCY_OPENING_BAL_AMT" ,
                    cast("OPCO_UOM_SK" as character varying(16777216)) as "OPCO_UOM_SK" ,
                    cast("OPCO_BRAND_SK" as character varying(16777216)) as "OPCO_BRAND_SK" ,
                    cast("OPCO_SUB_LEDGER_TYPE_SK" as character varying(16777216)) as "OPCO_SUB_LEDGER_TYPE_SK" ,
                    cast("SUB_LEDGER_CD" as character varying(16777216)) as "SUB_LEDGER_CD" 

            from OI_DATA_DEV_V2.intermediate_fnd_bkp.primex_opco_fscl_yr_gl_opening_bal

            
        )

        
)

select * from v_opco_fscl_yr_gl_opening_bal